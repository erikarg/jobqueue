// Package store persists jobs in PostgreSQL.
//
// Every state change is a single conditional UPDATE (compare-and-swap on the
// current status), so transitions stay correct with any number of workers or
// processes sharing the same database.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/erikarg/jobqueue/internal/job"
)

//go:embed schema.sql
var schema string

const columns = `id::text, type, payload, status, attempts, max_attempts, idempotency_key,
	run_at, locked_until, lease_token, last_error, created_at, updated_at, completed_at`

// migrationLockID serializes Migrate across instances starting at the same time;
// concurrent CREATE TABLE IF NOT EXISTS can otherwise fail on the catalog.
const migrationLockID = 7_331_001

// Store is the PostgreSQL job store.
type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Migrate applies the (idempotent) schema.
func (s *Store) Migrate(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.Ping(ctx)
}

// NewJob holds the client-provided fields of a job.
type NewJob struct {
	Type           string
	Payload        json.RawMessage
	MaxAttempts    int
	IdempotencyKey string // optional
}

// Create inserts a pending job. If the idempotency key was already used, it
// returns the existing job with created=false, or ErrIdempotencyConflict when
// the earlier request had a different type or payload.
func (s *Store) Create(ctx context.Context, nj NewJob) (j job.Job, created bool, err error) {
	j, err = scanJob(s.db.QueryRow(ctx, `
		INSERT INTO jobs (type, payload, max_attempts, idempotency_key, status)
		VALUES ($1, $2, $3, NULLIF($4, ''), 'pending')
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+columns,
		nj.Type, []byte(nj.Payload), nj.MaxAttempts, nj.IdempotencyKey))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, err
	}

	// ON CONFLICT DO NOTHING waits for a concurrent insert with the same key to
	// commit, so the row is visible here. jsonb comparison ignores key order and
	// whitespace.
	var sameRequest bool
	j, err = scanJob(s.db.QueryRow(ctx, `
		SELECT `+columns+`, type = $2 AND payload = $3::jsonb
		FROM jobs WHERE idempotency_key = $1`,
		nj.IdempotencyKey, nj.Type, []byte(nj.Payload)), &sameRequest)
	if err != nil {
		return job.Job{}, false, err
	}
	if !sameRequest {
		return job.Job{}, false, job.ErrIdempotencyConflict
	}
	return j, false, nil
}

// Get returns a job by ID.
func (s *Store) Get(ctx context.Context, id string) (job.Job, error) {
	j, err := scanJob(s.db.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id = $1`, id))
	return j, notFound(err)
}

// Claim atomically moves the next due pending job to processing, increments
// its attempt counter and lease token, and leases it for the given duration. ok is false when
// no job is due.
//
// FOR UPDATE SKIP LOCKED lets concurrent claimers pick different rows without
// blocking each other; a row can never be handed to two claimers.
func (s *Store) Claim(ctx context.Context, lease time.Duration) (j job.Job, ok bool, err error) {
	j, err = scanJob(s.db.QueryRow(ctx, `
		UPDATE jobs SET
			status = 'processing',
			attempts = attempts + 1,
			lease_token = lease_token + 1,
			locked_until = now() + $1 * interval '1 millisecond',
			updated_at = now()
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = 'pending' AND run_at <= now()
			ORDER BY run_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+columns, lease.Milliseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, nil
	}
	if err != nil {
		return job.Job{}, false, err
	}
	return j, true, nil
}

// The methods below report the outcome of a processing job. The lease token
// returned by Claim is a fencing token: the update only applies if the job is
// still processing under the same claim, so a worker whose lease expired
// cannot overwrite the result of a newer claim. They return ErrLeaseLost
// otherwise. (attempts can't serve as the token: RetryDead resets it and
// Release decrements it, so attempt numbers repeat.)

// Complete marks the job as completed.
func (s *Store) Complete(ctx context.Context, id string, token int64) error {
	return s.finish(ctx, id, token, `status = 'completed', completed_at = now(), last_error = NULL`)
}

// Retry schedules another attempt after delay. The due time is computed by the
// database clock, the same clock Claim compares against.
func (s *Store) Retry(ctx context.Context, id string, token int64, delay time.Duration, reason string) error {
	return s.finish(ctx, id, token,
		`status = 'pending', run_at = now() + $3 * interval '1 millisecond', last_error = $4`,
		delay.Milliseconds(), reason)
}

// Fail marks the job as failed (non-retryable error).
func (s *Store) Fail(ctx context.Context, id string, token int64, reason string) error {
	return s.finish(ctx, id, token, `status = 'failed', last_error = $3`, reason)
}

// DeadLetter marks the job as dead-lettered (retries exhausted).
func (s *Store) DeadLetter(ctx context.Context, id string, token int64, reason string) error {
	return s.finish(ctx, id, token, `status = 'dead_letter', last_error = $3`, reason)
}

// Release returns an interrupted job to pending without consuming an attempt.
// Used on shutdown, when the job did not fail on its own.
func (s *Store) Release(ctx context.Context, id string, token int64) error {
	return s.finish(ctx, id, token, `status = 'pending', attempts = attempts - 1, run_at = now()`)
}

func (s *Store) finish(ctx context.Context, id string, token int64, set string, args ...any) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE jobs SET `+set+`, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing' AND lease_token = $2`,
		append([]any{id, token}, args...)...)
	if err != nil {
		return notFound(err)
	}
	if tag.RowsAffected() == 0 {
		return job.ErrLeaseLost
	}
	return nil
}

// RetryDead moves a dead-lettered job back to pending with a fresh attempt
// budget. Any other status yields a *job.TransitionError.
func (s *Store) RetryDead(ctx context.Context, id string) (job.Job, error) {
	j, err := scanJob(s.db.QueryRow(ctx, `
		UPDATE jobs SET status = 'pending', attempts = 0, run_at = now(), updated_at = now()
		WHERE id = $1 AND status = 'dead_letter'
		RETURNING `+columns, id))
	if err == nil {
		return j, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, notFound(err)
	}
	current, err := s.Get(ctx, id)
	if err != nil {
		return job.Job{}, err
	}
	return job.Job{}, &job.TransitionError{From: current.Status, To: job.StatusPending}
}

// RecoverStale handles processing jobs whose lease expired, which means the
// worker that claimed them died (crash, OOM kill, network partition) without
// reporting a result. They return to pending, or go to dead_letter when the
// expired attempt was the last one.
func (s *Store) RecoverStale(ctx context.Context) ([]job.Job, error) {
	rows, err := s.db.Query(ctx, `
		UPDATE jobs SET
			status = CASE WHEN attempts >= max_attempts THEN 'dead_letter' ELSE 'pending' END,
			last_error = 'lease expired: worker stopped without reporting a result',
			run_at = now(),
			locked_until = NULL,
			updated_at = now()
		WHERE status = 'processing' AND locked_until < now()
		RETURNING `+columns)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (job.Job, error) {
		return scanJob(row)
	})
}

func scanJob(row pgx.Row, extra ...any) (job.Job, error) {
	var j job.Job
	dest := []any{
		&j.ID, &j.Type, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts, &j.IdempotencyKey,
		&j.RunAt, &j.LockedUntil, &j.LeaseToken, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.CompletedAt,
	}
	err := row.Scan(append(dest, extra...)...)
	return j, err
}

// notFound maps "no row" and malformed UUIDs to job.ErrNotFound.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return job.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "22P02" { // invalid_text_representation
		return job.ErrNotFound
	}
	return err
}
