// Package worker runs jobs from the store on a fixed-size pool of goroutines.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/erikarg/jobqueue/internal/job"
	"github.com/erikarg/jobqueue/internal/metrics"
)

// Store is the subset of the job store the pool needs. It is declared here,
// where it is consumed, so the pool can be unit-tested without a database.
type Store interface {
	Claim(ctx context.Context, lease time.Duration) (job.Job, bool, error)
	Complete(ctx context.Context, id string, token int64) error
	Retry(ctx context.Context, id string, token int64, delay time.Duration, reason string) error
	Fail(ctx context.Context, id string, token int64, reason string) error
	DeadLetter(ctx context.Context, id string, token int64, reason string) error
	Release(ctx context.Context, id string, token int64) error
	RecoverStale(ctx context.Context) ([]job.Job, error)
}

type Config struct {
	Workers          int           // number of concurrent jobs
	PollInterval     time.Duration // wait between claims when no job is due
	JobTimeout       time.Duration // per-attempt execution limit
	LeaseDuration    time.Duration // how long a claim is valid; must exceed JobTimeout + writeTimeout
	RecoveryInterval time.Duration // how often expired leases are recovered
	ShutdownTimeout  time.Duration // grace period for in-flight jobs on shutdown
	BackoffBase      time.Duration
	BackoffMax       time.Duration
}

func (c Config) validate() error {
	var errs []error
	if c.Workers < 1 {
		errs = append(errs, errors.New("workers must be at least 1"))
	}
	for name, d := range map[string]time.Duration{
		"poll interval": c.PollInterval, "job timeout": c.JobTimeout, "lease duration": c.LeaseDuration,
		"recovery interval": c.RecoveryInterval, "backoff base": c.BackoffBase,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if c.LeaseDuration <= c.JobTimeout+writeTimeout {
		errs = append(errs, fmt.Errorf("lease duration must exceed job timeout plus %v to record the outcome", writeTimeout))
	}
	if c.BackoffMax < c.BackoffBase {
		errs = append(errs, errors.New("backoff max must be >= backoff base"))
	}
	return errors.Join(errs...)
}

// writeTimeout bounds the store call that records a job's outcome.
const writeTimeout = 5 * time.Second

type Pool struct {
	store    Store
	handlers map[string]job.Handler
	cfg      Config
	log      *slog.Logger
	metrics  *metrics.Metrics
}

func New(store Store, handlers map[string]job.Handler, cfg Config, log *slog.Logger, m *metrics.Metrics) (*Pool, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid worker config: %w", err)
	}
	return &Pool{store: store, handlers: handlers, cfg: cfg, log: log, metrics: m}, nil
}

// Run processes jobs until ctx is cancelled, then shuts down gracefully:
// it stops claiming new jobs and waits up to ShutdownTimeout for in-flight
// jobs. Jobs still running after that are cancelled and released back to
// pending. Run returns once every worker has exited.
func (p *Pool) Run(ctx context.Context) {
	// Jobs run under execCtx, not ctx, so that cancelling ctx stops claiming
	// without interrupting jobs that are already running.
	execCtx, cancelExec := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelExec()

	// jobs is unbuffered and slots holds one token per busy worker. The
	// dispatcher takes a slot *before* claiming, so a job is only claimed when
	// a worker is free to start it immediately — claimed jobs never wait in a
	// queue while their lease runs down.
	jobs := make(chan job.Job)
	slots := make(chan struct{}, p.cfg.Workers)

	var workers sync.WaitGroup
	for range p.cfg.Workers {
		workers.Go(func() {
			for j := range jobs {
				p.execute(execCtx, j)
				<-slots
			}
		})
	}

	var recovery sync.WaitGroup
	recovery.Go(func() { p.recoverLoop(ctx) })

	p.dispatch(ctx, jobs, slots)
	close(jobs)
	recovery.Wait()

	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(p.cfg.ShutdownTimeout):
		p.log.Warn("shutdown timeout exceeded, cancelling in-flight jobs")
		cancelExec()
		<-done
	}
	p.log.Info("worker pool stopped")
}

func (p *Pool) dispatch(ctx context.Context, jobs chan<- job.Job, slots chan struct{}) {
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		// select picks randomly when both cases are ready, so re-check:
		// a free slot must not win over a shutdown that already started.
		if ctx.Err() != nil {
			return
		}

		j, ok, err := p.claim(ctx)
		if err != nil || !ok {
			<-slots
			if err != nil {
				p.log.Error("claim job", "error", err)
			}
			select {
			case <-time.After(p.cfg.PollInterval):
			case <-ctx.Done():
				return
			}
			continue
		}
		jobs <- j
	}
}

// claim is not cancelled by shutdown: aborting the query after the database
// committed the claim would strand the job until its lease expires.
func (p *Pool) claim(ctx context.Context) (job.Job, bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()
	return p.store.Claim(ctx, p.cfg.LeaseDuration)
}

// execute runs one attempt and records its outcome.
func (p *Pool) execute(ctx context.Context, j job.Job) {
	log := p.log.With("job_id", j.ID, "job_type", j.Type, "attempt", j.Attempts, "max_attempts", j.MaxAttempts)
	log.Info("job started")

	start := time.Now()
	err := p.run(ctx, j, log)
	elapsed := time.Since(start)

	// Record the outcome even if ctx was cancelled by shutdown.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	defer cancel()

	var (
		outcome string
		werr    error
		delay   time.Duration
	)
	switch {
	case err == nil:
		outcome, werr = "completed", p.store.Complete(wctx, j.ID, j.LeaseToken)
	case ctx.Err() != nil:
		// Interrupted by shutdown: not the job's fault, so no attempt is consumed.
		outcome, werr = "released", p.store.Release(wctx, j.ID, j.LeaseToken)
	case job.IsPermanent(err):
		outcome, werr = "failed", p.store.Fail(wctx, j.ID, j.LeaseToken, err.Error())
	case j.Attempts >= j.MaxAttempts:
		outcome, werr = "dead_letter", p.store.DeadLetter(wctx, j.ID, j.LeaseToken, err.Error())
	default:
		delay = backoff(j.Attempts, p.cfg.BackoffBase, p.cfg.BackoffMax)
		outcome, werr = "retried", p.store.Retry(wctx, j.ID, j.LeaseToken, delay, err.Error())
	}

	if werr != nil {
		// ErrLeaseLost: the lease expired while running and the job was
		// recovered; the newer attempt owns the outcome now.
		log.Error("record job outcome", "outcome", outcome, "error", werr, "job_error", err)
		return
	}

	p.metrics.Duration.WithLabelValues(outcome).Observe(elapsed.Seconds())
	attrs := []any{"outcome", outcome, "duration_ms", elapsed.Milliseconds()}
	switch outcome {
	case "completed":
		p.metrics.Completed.Inc()
		log.Info("job finished", attrs...)
	case "failed", "dead_letter":
		p.metrics.Failed.WithLabelValues(outcome).Inc()
		log.Error("job finished", append(attrs, "error", err)...)
	case "retried":
		p.metrics.Retried.Inc()
		log.Warn("job finished", append(attrs, "error", err, "retry_in", delay.String())...)
	case "released":
		log.Warn("job finished", attrs...)
	}
}

// run invokes the handler under the per-job timeout and converts panics into
// (retryable) errors so a buggy handler cannot take down the worker.
func (p *Pool) run(ctx context.Context, j job.Job, log *slog.Logger) (err error) {
	handler, ok := p.handlers[j.Type]
	if !ok {
		return job.Permanent(fmt.Errorf("no handler registered for job type %q", j.Type))
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.JobTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			log.Error("job handler panicked", "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, j.Payload)
}

func (p *Pool) recoverLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.RecoveryInterval)
	defer ticker.Stop()
	for {
		recovered, err := p.store.RecoverStale(ctx)
		if err != nil && ctx.Err() == nil {
			p.log.Error("recover stale jobs", "error", err)
		}
		for _, j := range recovered {
			p.metrics.Recovered.Inc()
			if j.Status == job.StatusDeadLetter {
				p.metrics.Failed.WithLabelValues(string(job.StatusDeadLetter)).Inc()
			}
			p.log.Warn("recovered job with expired lease", "job_id", j.ID, "job_type", j.Type, "attempt", j.Attempts, "status", j.Status)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}
