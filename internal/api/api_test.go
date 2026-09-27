package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/erikarg/jobqueue/internal/api"
	"github.com/erikarg/jobqueue/internal/job"
	"github.com/erikarg/jobqueue/internal/metrics"
	"github.com/erikarg/jobqueue/internal/pgtest"
	"github.com/erikarg/jobqueue/internal/store"
	"github.com/erikarg/jobqueue/internal/worker"
)

type env struct {
	url     string
	metrics *metrics.Metrics
	server  *api.Server
}

// setup starts the API and a worker pool against an isolated database schema.
func setup(t *testing.T, handlers map[string]job.Handler) env {
	t.Helper()
	db := pgtest.New(t)
	st := store.New(db)
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	pool, err := worker.New(st, handlers, worker.Config{
		Workers:          2,
		PollInterval:     10 * time.Millisecond,
		JobTimeout:       time.Second,
		LeaseDuration:    10 * time.Second,
		RecoveryInterval: time.Second,
		ShutdownTimeout:  time.Second,
		BackoffBase:      10 * time.Millisecond,
		BackoffMax:       20 * time.Millisecond,
	}, log, m)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pool.Run(ctx); close(done) }()

	srv := &api.Server{Store: st, Handlers: handlers, MaxAttempts: 3, Metrics: m, Gatherer: reg, Log: log}
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(func() {
		ts.Close()
		cancel()
		<-done
	})
	return env{url: ts.URL, metrics: m, server: srv}
}

func do(t *testing.T, method, url, body string, header ...string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func waitForStatus(t *testing.T, base, id string, want job.Status) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, body := do(t, http.MethodGet, base+"/jobs/"+id+"/status", "")
		if body["status"] == string(want) {
			return body
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s: status %v, want %s", id, body["status"], want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSubmittedJobIsProcessedToCompletion(t *testing.T) {
	e := setup(t, map[string]job.Handler{
		"echo": func(context.Context, json.RawMessage) error { return nil },
	})

	code, body := do(t, http.MethodPost, e.url+"/jobs", `{"type":"echo","payload":{"hello":"world"}}`)
	if code != http.StatusCreated || body["status"] != "pending" {
		t.Fatalf("create: %d %v", code, body)
	}
	id := body["id"].(string)

	status := waitForStatus(t, e.url, id, job.StatusCompleted)
	if status["attempts"] != float64(1) {
		t.Fatalf("attempts = %v, want 1", status["attempts"])
	}

	code, full := do(t, http.MethodGet, e.url+"/jobs/"+id, "")
	if code != http.StatusOK || full["completed_at"] == nil {
		t.Fatalf("get: %d %v", code, full)
	}
	if got := testutil.ToFloat64(e.metrics.Submitted); got != 1 {
		t.Fatalf("submitted metric = %v, want 1", got)
	}
}

func TestIdempotencyKeyPreventsDuplicateSubmission(t *testing.T) {
	e := setup(t, map[string]job.Handler{
		"echo": func(context.Context, json.RawMessage) error { return nil },
	})
	body := `{"type":"echo","payload":{"order":42}}`

	code1, first := do(t, http.MethodPost, e.url+"/jobs", body, "Idempotency-Key", "order-42")
	code2, second := do(t, http.MethodPost, e.url+"/jobs", body, "Idempotency-Key", "order-42")
	if code1 != http.StatusCreated || code2 != http.StatusOK || first["id"] != second["id"] {
		t.Fatalf("got %d/%v and %d/%v, want 201 then 200 with the same job", code1, first["id"], code2, second["id"])
	}

	code, _ := do(t, http.MethodPost, e.url+"/jobs", `{"type":"echo","payload":{"order":43}}`, "Idempotency-Key", "order-42")
	if code != http.StatusConflict {
		t.Fatalf("reused key with different payload: got %d, want 409", code)
	}
	if got := testutil.ToFloat64(e.metrics.Submitted); got != 1 {
		t.Fatalf("submitted metric = %v, want 1", got)
	}
}

func TestExhaustedJobIsDeadLetteredAndCanBeRetried(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	calls := make(chan struct{}, 10)
	e := setup(t, map[string]job.Handler{
		"flaky": func(context.Context, json.RawMessage) error {
			calls <- struct{}{}
			if fail.Load() {
				return errors.New("downstream unavailable")
			}
			return nil
		},
	})

	_, body := do(t, http.MethodPost, e.url+"/jobs", `{"type":"flaky","max_attempts":2}`)
	id := body["id"].(string)
	status := waitForStatus(t, e.url, id, job.StatusDeadLetter)
	if status["attempts"] != float64(2) || status["last_error"] != "downstream unavailable" {
		t.Fatalf("unexpected dead-lettered status: %v", status)
	}
	if len(calls) != 2 {
		t.Fatalf("handler ran %d times, want 2", len(calls))
	}

	fail.Store(false)
	code, body := do(t, http.MethodPost, e.url+"/jobs/"+id+"/retry", "")
	if code != http.StatusOK || body["status"] != "pending" || body["attempts"] != float64(0) {
		t.Fatalf("retry: %d %v", code, body)
	}
	waitForStatus(t, e.url, id, job.StatusCompleted)
}

func TestRetryingANonDeadLetteredJobConflicts(t *testing.T) {
	e := setup(t, map[string]job.Handler{
		"echo": func(context.Context, json.RawMessage) error { return nil },
	})
	_, body := do(t, http.MethodPost, e.url+"/jobs", `{"type":"echo"}`)
	id := body["id"].(string)
	waitForStatus(t, e.url, id, job.StatusCompleted)

	code, body := do(t, http.MethodPost, e.url+"/jobs/"+id+"/retry", "")
	if code != http.StatusConflict {
		t.Fatalf("got %d %v, want 409", code, body)
	}
}

func TestPermanentErrorFailsJobWithoutRetry(t *testing.T) {
	e := setup(t, map[string]job.Handler{
		"strict": func(context.Context, json.RawMessage) error {
			return job.Permanent(errors.New("invalid recipient"))
		},
	})
	_, body := do(t, http.MethodPost, e.url+"/jobs", `{"type":"strict"}`)
	status := waitForStatus(t, e.url, body["id"].(string), job.StatusFailed)
	if status["attempts"] != float64(1) {
		t.Fatalf("attempts = %v, want 1", status["attempts"])
	}
}

func TestCreateRejectsInvalidRequests(t *testing.T) {
	e := setup(t, map[string]job.Handler{
		"echo": func(context.Context, json.RawMessage) error { return nil },
	})
	for name, body := range map[string]string{
		"malformed json":   `{"type":`,
		"unknown type":     `{"type":"nope"}`,
		"unknown field":    `{"type":"echo","priority":1}`,
		"too many retries": `{"type":"echo","max_attempts":1000}`,
		"negative retries": `{"type":"echo","max_attempts":-1}`,
	} {
		if code, _ := do(t, http.MethodPost, e.url+"/jobs", body); code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", name, code)
		}
	}
}

func TestUnknownJobIsNotFound(t *testing.T) {
	e := setup(t, nil)
	for _, path := range []string{"/jobs/00000000-0000-0000-0000-000000000000", "/jobs/not-a-uuid/status"} {
		if code, _ := do(t, http.MethodGet, e.url+path, ""); code != http.StatusNotFound {
			t.Errorf("GET %s: got %d, want 404", path, code)
		}
	}
}

func TestHealthAndReadiness(t *testing.T) {
	e := setup(t, nil)
	if code, _ := do(t, http.MethodGet, e.url+"/healthz", ""); code != http.StatusOK {
		t.Fatalf("healthz: %d", code)
	}
	if code, _ := do(t, http.MethodGet, e.url+"/readyz", ""); code != http.StatusOK {
		t.Fatalf("readyz: %d", code)
	}
	e.server.SetShuttingDown()
	if code, _ := do(t, http.MethodGet, e.url+"/readyz", ""); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz while shutting down: %d, want 503", code)
	}
}

func TestRequestIDIsPropagated(t *testing.T) {
	e := setup(t, nil)
	req, _ := http.NewRequest(http.MethodGet, e.url+"/healthz", nil)
	req.Header.Set("X-Request-ID", "abc-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Request-ID"); got != "abc-123" {
		t.Fatalf("X-Request-ID = %q, want abc-123", got)
	}
}
