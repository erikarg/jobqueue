// Package api exposes the job store over HTTP.
package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/erikarg/jobqueue/internal/job"
	"github.com/erikarg/jobqueue/internal/metrics"
	"github.com/erikarg/jobqueue/internal/store"
)

const (
	maxBodyBytes       = 1 << 20
	maxAllowedAttempts = 100
)

// Server holds the HTTP handlers' dependencies.
type Server struct {
	Store       *store.Store
	Handlers    map[string]job.Handler // used to reject unknown job types
	MaxAttempts int                    // default when the request omits max_attempts
	Metrics     *metrics.Metrics
	Gatherer    prometheus.Gatherer
	Log         *slog.Logger

	shuttingDown atomic.Bool
}

// Routes returns the HTTP handler for the service.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /jobs/{id}/status", s.getJobStatus)
	mux.HandleFunc("POST /jobs/{id}/retry", s.retryJob)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.Gatherer, promhttp.HandlerOpts{}))
	return s.withRequestLogging(mux)
}

// SetShuttingDown makes /readyz fail so load balancers stop routing traffic
// here before the server closes its listener.
func (s *Server) SetShuttingDown() {
	s.shuttingDown.Store(true)
}

type createJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if _, ok := s.Handlers[req.Type]; !ok {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown job type %q", req.Type))
		return
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = s.MaxAttempts
	}
	if req.MaxAttempts < 1 || req.MaxAttempts > maxAllowedAttempts {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("max_attempts must be between 1 and %d", maxAllowedAttempts))
		return
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		req.Payload = json.RawMessage(`{}`)
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 255 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key must be at most 255 characters")
		return
	}

	j, created, err := s.Store.Create(r.Context(), store.NewJob{
		Type: req.Type, Payload: req.Payload, MaxAttempts: req.MaxAttempts, IdempotencyKey: key,
	})
	switch {
	case errors.Is(err, job.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		s.internalError(w, r, "create job", err)
		return
	}

	log := s.logger(r).With("job_id", j.ID, "job_type", j.Type)
	status := http.StatusOK // idempotent replay: same response as the original request
	if created {
		status = http.StatusCreated
		s.Metrics.Submitted.Inc()
		log.Info("job submitted")
	} else {
		log.Info("idempotent replay of existing job")
	}
	w.Header().Set("Location", "/jobs/"+j.ID)
	writeJSON(w, status, j)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	j, ok := s.lookup(w, r)
	if ok {
		writeJSON(w, http.StatusOK, j)
	}
}

type jobStatus struct {
	ID          string     `json:"id"`
	Status      job.Status `json:"status"`
	Attempts    int        `json:"attempts"`
	MaxAttempts int        `json:"max_attempts"`
	RunAt       time.Time  `json:"run_at"`
	LastError   *string    `json:"last_error,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (s *Server) getJobStatus(w http.ResponseWriter, r *http.Request) {
	j, ok := s.lookup(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, jobStatus{
		ID: j.ID, Status: j.Status, Attempts: j.Attempts, MaxAttempts: j.MaxAttempts,
		RunAt: j.RunAt, LastError: j.LastError, UpdatedAt: j.UpdatedAt,
	})
}

func (s *Server) lookup(w http.ResponseWriter, r *http.Request) (job.Job, bool) {
	j, err := s.Store.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
		return job.Job{}, false
	case err != nil:
		s.internalError(w, r, "get job", err)
		return job.Job{}, false
	}
	return j, true
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.Store.RetryDead(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
		return
	case errors.Is(err, job.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "only dead-lettered jobs can be retried: "+err.Error())
		return
	case err != nil:
		s.internalError(w, r, "retry job", err)
		return
	}
	s.logger(r).Info("dead-lettered job requeued", "job_id", j.ID, "job_type", j.Type)
	writeJSON(w, http.StatusOK, j)
}

// healthz is the liveness probe: the process is up and serving HTTP.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is the readiness probe: the service can do useful work.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown.Load() {
		writeError(w, http.StatusServiceUnavailable, "shutting down")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		s.logger(r).Warn("readiness check failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, msg string, err error) {
	s.logger(r).Error(msg, "error", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false) // this is a JSON API, not HTML
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// Request IDs and access logging.

type ctxKey struct{}

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func (s *Server) logger(r *http.Request) *slog.Logger {
	return s.Log.With("request_id", requestID(r.Context()))
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(status int) {
	rec.status = status
	rec.ResponseWriter.WriteHeader(status)
}

// withRequestLogging assigns a request ID (reusing a sane X-Request-ID from the
// caller), echoes it in the response and logs one line per request.
func (s *Server) withRequestLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			id = rand.Text()
		}
		w.Header().Set("X-Request-ID", id)
		r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, id))

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		s.logger(r).Info("http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}
