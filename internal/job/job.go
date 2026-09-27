// Package job defines the job domain model, its lifecycle state machine and
// the error classification used by handlers.
package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Status is the lifecycle state of a job.
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"      // non-retryable error
	StatusDeadLetter Status = "dead_letter" // retries exhausted
)

// transitions is the single source of truth for the lifecycle:
//
//	pending     -> processing                    (claimed by a worker)
//	processing  -> completed                     (handler succeeded)
//	processing  -> pending                       (retry scheduled, shutdown release, lease recovery)
//	processing  -> failed                        (non-retryable error)
//	processing  -> dead_letter                   (retries exhausted)
//	dead_letter -> pending                       (manual retry via API)
//
// completed and failed are terminal.
var transitions = map[Status][]Status{
	StatusPending:    {StatusProcessing},
	StatusProcessing: {StatusCompleted, StatusPending, StatusFailed, StatusDeadLetter},
	StatusDeadLetter: {StatusPending},
}

// CanTransition reports whether the lifecycle allows moving from one status to another.
func CanTransition(from, to Status) bool {
	return slices.Contains(transitions[from], to)
}

// Job is a unit of work persisted in the job store.
type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	RunAt          time.Time       `json:"run_at"`
	LockedUntil    *time.Time      `json:"locked_until,omitempty"`
	LeaseToken     int64           `json:"-"` // fencing token, incremented on every claim
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
}

// Handler executes a job. It must honor ctx: when ctx is done (timeout or
// shutdown) the handler should stop and return promptly.
//
// Returned errors are retried by default; wrap them with Permanent to fail
// the job immediately.
type Handler func(ctx context.Context, payload json.RawMessage) error

var (
	ErrNotFound            = errors.New("job not found")
	ErrInvalidTransition   = errors.New("invalid state transition")
	ErrIdempotencyConflict = errors.New("idempotency key already used with a different request")

	// ErrLeaseLost is returned when a worker reports the outcome of a job it no
	// longer owns (its lease expired and the job was recovered or re-claimed).
	ErrLeaseLost = errors.New("job lease lost")
)

// TransitionError describes a rejected state transition.
type TransitionError struct {
	From, To Status
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s: %s -> %s", ErrInvalidTransition, e.From, e.To)
}

func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable: the job moves straight to failed.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or any error it wraps) was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
