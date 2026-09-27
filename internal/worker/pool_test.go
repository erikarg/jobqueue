package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/erikarg/jobqueue/internal/job"
	"github.com/erikarg/jobqueue/internal/metrics"
)

// outcome is what the pool reported to the store for one attempt.
type outcome struct {
	id      string
	attempt int
	kind    string // complete | retry | fail | dead_letter | release
	delay   time.Duration
}

// fakeStore hands out queued jobs and records reported outcomes. Retried and
// released jobs are re-queued immediately (the delay is only recorded).
type fakeStore struct {
	mu        sync.Mutex
	pending   []job.Job
	claims    int
	recovered int
	outcomes  chan outcome
}

func newFakeStore(jobs ...job.Job) *fakeStore {
	return &fakeStore{pending: jobs, outcomes: make(chan outcome, 1000)}
}

func (f *fakeStore) Claim(ctx context.Context, lease time.Duration) (job.Job, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) == 0 {
		return job.Job{}, false, nil
	}
	j := f.pending[0]
	f.pending = f.pending[1:]
	j.Status = job.StatusProcessing
	j.Attempts++
	j.LeaseToken = int64(j.Attempts) // unique per claim in the fake: it never resets attempts
	f.claims++
	return j, true, nil
}

func (f *fakeStore) record(o outcome, requeue *job.Job) error {
	if requeue != nil {
		f.mu.Lock()
		f.pending = append(f.pending, *requeue)
		f.mu.Unlock()
	}
	f.outcomes <- o
	return nil
}

func (f *fakeStore) Complete(_ context.Context, id string, token int64) error {
	return f.record(outcome{id: id, attempt: int(token), kind: "complete"}, nil)
}

func (f *fakeStore) Retry(_ context.Context, id string, token int64, delay time.Duration, _ string) error {
	return f.record(outcome{id: id, attempt: int(token), kind: "retry", delay: delay},
		&job.Job{ID: id, Type: typeOf(id), Attempts: int(token), MaxAttempts: maxAttempts})
}

func (f *fakeStore) Fail(_ context.Context, id string, token int64, _ string) error {
	return f.record(outcome{id: id, attempt: int(token), kind: "fail"}, nil)
}

func (f *fakeStore) DeadLetter(_ context.Context, id string, token int64, _ string) error {
	return f.record(outcome{id: id, attempt: int(token), kind: "dead_letter"}, nil)
}

func (f *fakeStore) Release(_ context.Context, id string, token int64) error {
	return f.record(outcome{id: id, attempt: int(token), kind: "release"}, nil)
}

func (f *fakeStore) RecoverStale(context.Context) ([]job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recovered++
	return nil, nil
}

func (f *fakeStore) claimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims
}

const maxAttempts = 3

// Job IDs are "<type>-<n>" so the fake can rebuild a job on retry.
func typeOf(id string) string {
	return id[:strings.LastIndex(id, "-")]
}

func newJob(typ string, n int) job.Job {
	return job.Job{ID: fmt.Sprintf("%s-%d", typ, n), Type: typ, Payload: json.RawMessage(`{}`), Status: job.StatusPending, MaxAttempts: maxAttempts}
}

func testConfig() Config {
	return Config{
		Workers:          2,
		PollInterval:     5 * time.Millisecond,
		JobTimeout:       time.Second,
		LeaseDuration:    10 * time.Second,
		RecoveryInterval: time.Hour,
		ShutdownTimeout:  time.Second,
		BackoffBase:      10 * time.Millisecond,
		BackoffMax:       40 * time.Millisecond,
	}
}

// startPool runs a pool in the background and returns a stop function that
// cancels it and waits for Run to return.
func startPool(t *testing.T, store Store, handlers map[string]job.Handler, cfg Config) (*metrics.Metrics, func()) {
	t.Helper()
	m := metrics.New(prometheus.NewRegistry())
	p, err := New(store, handlers, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(stopped)
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("pool did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return m, stop
}

func next(t *testing.T, f *fakeStore) outcome {
	t.Helper()
	select {
	case o := <-f.outcomes:
		return o
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job outcome")
		return outcome{}
	}
}

func TestSuccessfulJobIsCompleted(t *testing.T) {
	f := newFakeStore(newJob("ok", 1))
	m, stop := startPool(t, f, map[string]job.Handler{
		"ok": func(context.Context, json.RawMessage) error { return nil },
	}, testConfig())

	if o := next(t, f); o.kind != "complete" || o.attempt != 1 {
		t.Fatalf("got %+v, want complete on attempt 1", o)
	}
	stop() // metrics are updated after the outcome is stored
	if got := testutil.ToFloat64(m.Completed); got != 1 {
		t.Fatalf("completed metric = %v, want 1", got)
	}
}

func TestRetryableErrorIsRetriedWithBackoffUntilSuccess(t *testing.T) {
	var calls atomic.Int32
	f := newFakeStore(newJob("flaky", 1))
	cfg := testConfig()
	m, stop := startPool(t, f, map[string]job.Handler{
		"flaky": func(context.Context, json.RawMessage) error {
			if calls.Add(1) < 3 {
				return errors.New("temporary")
			}
			return nil
		},
	}, cfg)

	for attempt := 1; attempt <= 2; attempt++ {
		o := next(t, f)
		ceiling := min(cfg.BackoffBase<<(attempt-1), cfg.BackoffMax)
		if o.kind != "retry" || o.attempt != attempt || o.delay < ceiling/2 || o.delay > ceiling {
			t.Fatalf("attempt %d: got %+v, want retry with delay in [%v, %v]", attempt, o, ceiling/2, ceiling)
		}
	}
	if o := next(t, f); o.kind != "complete" || o.attempt != 3 {
		t.Fatalf("got %+v, want complete on attempt 3", o)
	}
	stop()
	if got := testutil.ToFloat64(m.Retried); got != 2 {
		t.Fatalf("retried metric = %v, want 2", got)
	}
}

func TestRetryableErrorOnLastAttemptDeadLetters(t *testing.T) {
	j := newJob("broken", 1)
	j.Attempts = maxAttempts - 1 // next claim is the last attempt
	f := newFakeStore(j)
	m, stop := startPool(t, f, map[string]job.Handler{
		"broken": func(context.Context, json.RawMessage) error { return errors.New("still down") },
	}, testConfig())

	if o := next(t, f); o.kind != "dead_letter" || o.attempt != maxAttempts {
		t.Fatalf("got %+v, want dead_letter on attempt %d", o, maxAttempts)
	}
	stop()
	if got := testutil.ToFloat64(m.Failed.WithLabelValues("dead_letter")); got != 1 {
		t.Fatalf("dead_letter metric = %v, want 1", got)
	}
}

func TestPermanentErrorFailsWithoutRetry(t *testing.T) {
	f := newFakeStore(newJob("invalid", 1))
	startPool(t, f, map[string]job.Handler{
		"invalid": func(context.Context, json.RawMessage) error {
			return fmt.Errorf("validate: %w", job.Permanent(errors.New("bad payload")))
		},
	}, testConfig())

	if o := next(t, f); o.kind != "fail" || o.attempt != 1 {
		t.Fatalf("got %+v, want fail on attempt 1", o)
	}
}

func TestUnknownJobTypeFailsPermanently(t *testing.T) {
	f := newFakeStore(newJob("unknown", 1))
	startPool(t, f, map[string]job.Handler{}, testConfig())

	if o := next(t, f); o.kind != "fail" {
		t.Fatalf("got %+v, want fail", o)
	}
}

func TestPanickingHandlerIsRetriedAndWorkerSurvives(t *testing.T) {
	var calls atomic.Int32
	f := newFakeStore(newJob("panics", 1))
	startPool(t, f, map[string]job.Handler{
		"panics": func(context.Context, json.RawMessage) error {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			return nil
		},
	}, testConfig())

	if o := next(t, f); o.kind != "retry" {
		t.Fatalf("got %+v, want retry after panic", o)
	}
	if o := next(t, f); o.kind != "complete" {
		t.Fatalf("got %+v, want complete", o)
	}
}

func TestJobTimeoutCancelsHandlerAndRetries(t *testing.T) {
	f := newFakeStore(newJob("slow", 1))
	cfg := testConfig()
	cfg.JobTimeout = 20 * time.Millisecond
	var sawDeadline atomic.Bool
	startPool(t, f, map[string]job.Handler{
		"slow": func(ctx context.Context, _ json.RawMessage) error {
			<-ctx.Done()
			sawDeadline.Store(errors.Is(ctx.Err(), context.DeadlineExceeded))
			return ctx.Err()
		},
	}, cfg)

	if o := next(t, f); o.kind != "retry" {
		t.Fatalf("got %+v, want retry after timeout", o)
	}
	if !sawDeadline.Load() {
		t.Fatal("handler context was not cancelled by the job timeout")
	}
}

func TestPoolNeverExceedsConfiguredConcurrency(t *testing.T) {
	const jobs, workers = 30, 3
	var queued []job.Job
	for i := range jobs {
		queued = append(queued, newJob("work", i))
	}
	f := newFakeStore(queued...)
	cfg := testConfig()
	cfg.Workers = workers

	var running, peak atomic.Int32
	startPool(t, f, map[string]job.Handler{
		"work": func(context.Context, json.RawMessage) error {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			running.Add(-1)
			return nil
		},
	}, cfg)

	seen := map[string]bool{}
	for range jobs {
		o := next(t, f)
		if o.kind != "complete" || seen[o.id] {
			t.Fatalf("unexpected outcome %+v", o)
		}
		seen[o.id] = true
	}
	if p := peak.Load(); p > workers {
		t.Fatalf("peak concurrency %d exceeds %d workers", p, workers)
	}
	if p := peak.Load(); p < 2 {
		t.Fatalf("peak concurrency %d: jobs did not run in parallel", p)
	}
}

func TestGracefulShutdownFinishesInFlightJobsAndStopsClaiming(t *testing.T) {
	started := make(chan struct{}, 2)
	f := newFakeStore(newJob("slow", 1))
	cfg := testConfig()
	cfg.Workers = 1
	_, stop := startPool(t, f, map[string]job.Handler{
		"slow": func(ctx context.Context, _ json.RawMessage) error {
			started <- struct{}{}
			select {
			case <-time.After(50 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}, cfg)

	<-started
	// Queue a second job while the only worker is busy. When the first job
	// finishes during the grace period, the freed worker must not claim it.
	f.mu.Lock()
	f.pending = append(f.pending, newJob("slow", 2))
	f.mu.Unlock()
	stop() // blocks until Run returns

	if o := next(t, f); o.kind != "complete" || o.id != "slow-1" {
		t.Fatalf("got %+v, want in-flight job to complete during shutdown", o)
	}
	if n := f.claimCount(); n != 1 {
		t.Fatalf("pool made %d claims, want 1: it claimed after shutdown began", n)
	}
}

func TestShutdownTimeoutCancelsAndReleasesInFlightJobs(t *testing.T) {
	started := make(chan struct{})
	f := newFakeStore(newJob("stuck", 1))
	cfg := testConfig()
	cfg.ShutdownTimeout = 20 * time.Millisecond
	_, stop := startPool(t, f, map[string]job.Handler{
		"stuck": func(ctx context.Context, _ json.RawMessage) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}, cfg)

	<-started
	stop()

	if o := next(t, f); o.kind != "release" || o.attempt != 1 {
		t.Fatalf("got %+v, want release of attempt 1", o)
	}
}

func TestRecoveryRunsOnStartup(t *testing.T) {
	f := newFakeStore()
	_, stop := startPool(t, f, nil, testConfig())
	stop() // Run waits for the recovery loop, which always runs once first

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recovered == 0 {
		t.Fatal("RecoverStale was not called on startup")
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cfg := testConfig()
	cfg.LeaseDuration = cfg.JobTimeout // lease must outlive the job timeout
	cfg.Workers = 0
	if _, err := New(newFakeStore(), nil, cfg, slog.Default(), nil); err == nil {
		t.Fatal("expected invalid config error")
	}
}
