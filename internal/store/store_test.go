package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/erikarg/jobqueue/internal/job"
	"github.com/erikarg/jobqueue/internal/pgtest"
	"github.com/erikarg/jobqueue/internal/store"
)

const lease = time.Minute

func newStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	db := pgtest.New(t)
	s := store.New(db)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s, db
}

func create(t *testing.T, s *store.Store, key string) job.Job {
	t.Helper()
	j, _, err := s.Create(context.Background(), store.NewJob{
		Type: "test", Payload: json.RawMessage(`{"n":1}`), MaxAttempts: 3, IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return j
}

func claim(t *testing.T, s *store.Store) job.Job {
	t.Helper()
	j, ok, err := s.Claim(context.Background(), lease)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	return j
}

func expireLeases(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	if _, err := db.Exec(context.Background(),
		`UPDATE jobs SET locked_until = now() - interval '1 second' WHERE status = 'processing'`); err != nil {
		t.Fatal(err)
	}
}

func TestCreateWithSameIdempotencyKeyReturnsExistingJob(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()

	first, created, err := s.Create(ctx, store.NewJob{
		Type: "test", Payload: json.RawMessage(`{"a":1,"b":2}`), MaxAttempts: 3, IdempotencyKey: "k1",
	})
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}

	// Same JSON with different key order and whitespace is the same request.
	second, created, err := s.Create(ctx, store.NewJob{
		Type: "test", Payload: json.RawMessage(`{ "b": 2, "a": 1 }`), MaxAttempts: 3, IdempotencyKey: "k1",
	})
	if err != nil || created {
		t.Fatalf("second create: created=%v err=%v", created, err)
	}
	if second.ID != first.ID {
		t.Fatalf("got job %s, want existing %s", second.ID, first.ID)
	}
}

func TestCreateWithReusedKeyAndDifferentPayloadConflicts(t *testing.T) {
	s, _ := newStore(t)
	create(t, s, "k1")

	_, _, err := s.Create(context.Background(), store.NewJob{
		Type: "test", Payload: json.RawMessage(`{"n":2}`), MaxAttempts: 3, IdempotencyKey: "k1",
	})
	if !errors.Is(err, job.ErrIdempotencyConflict) {
		t.Fatalf("got %v, want ErrIdempotencyConflict", err)
	}
}

func TestConcurrentCreatesWithSameKeyInsertOnce(t *testing.T) {
	s, _ := newStore(t)
	const n = 20

	var wg sync.WaitGroup
	ids := make(chan string, n)
	createdCount := make(chan bool, n)
	for range n {
		wg.Go(func() {
			j, created, err := s.Create(context.Background(), store.NewJob{
				Type: "test", Payload: json.RawMessage(`{}`), MaxAttempts: 3, IdempotencyKey: "same",
			})
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ids <- j.ID
			createdCount <- created
		})
	}
	wg.Wait()
	close(ids)
	close(createdCount)

	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	inserted := 0
	for c := range createdCount {
		if c {
			inserted++
		}
	}
	if len(unique) != 1 || inserted != 1 {
		t.Fatalf("got %d distinct jobs and %d inserts, want 1 and 1", len(unique), inserted)
	}
}

func TestConcurrentClaimsNeverReturnTheSameJob(t *testing.T) {
	s, _ := newStore(t)
	const jobs, claimers = 100, 10
	for range jobs {
		create(t, s, "")
	}

	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	for range claimers {
		wg.Go(func() {
			for {
				j, ok, err := s.Claim(context.Background(), lease)
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				claimed[j.ID]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(claimed) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Errorf("job %s claimed %d times", id, n)
		}
	}
}

func TestClaimIncrementsAttemptsAndSetsLease(t *testing.T) {
	s, _ := newStore(t)
	create(t, s, "")

	j := claim(t, s)
	if j.Status != job.StatusProcessing || j.Attempts != 1 || j.LockedUntil == nil {
		t.Fatalf("unexpected claimed job: status=%s attempts=%d locked_until=%v", j.Status, j.Attempts, j.LockedUntil)
	}
}

func TestRetriedJobIsNotClaimableBeforeItsDelay(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	create(t, s, "")
	j := claim(t, s)

	if err := s.Retry(ctx, j.ID, j.LeaseToken, time.Hour, "boom"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Claim(ctx, lease); err != nil || ok {
		t.Fatalf("job claimed before its run_at: ok=%v err=%v", ok, err)
	}

	got, err := s.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != job.StatusPending || got.LastError == nil || *got.LastError != "boom" {
		t.Fatalf("unexpected job after retry: %+v", got)
	}
}

func TestReleaseDoesNotConsumeAnAttempt(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	create(t, s, "")
	j := claim(t, s)

	if err := s.Release(ctx, j.ID, j.LeaseToken); err != nil {
		t.Fatal(err)
	}
	again := claim(t, s)
	if again.ID != j.ID || again.Attempts != 1 {
		t.Fatalf("got job %s attempt %d, want %s attempt 1", again.ID, again.Attempts, j.ID)
	}
}

func TestRecoverStaleRequeuesOrDeadLettersExpiredLeases(t *testing.T) {
	s, db := newStore(t)
	ctx := context.Background()

	requeue := create(t, s, "requeue")
	claim(t, s)
	exhausted := create(t, s, "exhausted")
	claim(t, s)
	if _, err := db.Exec(ctx, `UPDATE jobs SET attempts = max_attempts WHERE id = $1`, exhausted.ID); err != nil {
		t.Fatal(err)
	}
	expireLeases(t, db)

	healthy := create(t, s, "healthy")
	claim(t, s) // lease still valid: must not be touched

	recovered, err := s.RecoverStale(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 2 {
		t.Fatalf("recovered %d jobs, want 2", len(recovered))
	}

	for id, want := range map[string]job.Status{
		requeue.ID:   job.StatusPending,
		exhausted.ID: job.StatusDeadLetter,
		healthy.ID:   job.StatusProcessing,
	} {
		got, err := s.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != want {
			t.Errorf("job %s: status %s, want %s", id, got.Status, want)
		}
	}
}

func TestStaleWorkerCannotOverwriteNewerAttempt(t *testing.T) {
	s, db := newStore(t)
	ctx := context.Background()
	create(t, s, "")

	stale := claim(t, s)
	expireLeases(t, db)
	if _, err := s.RecoverStale(ctx); err != nil {
		t.Fatal(err)
	}
	current := claim(t, s)
	if current.Attempts != stale.Attempts+1 {
		t.Fatalf("got attempt %d, want %d", current.Attempts, stale.Attempts+1)
	}

	if err := s.Complete(ctx, stale.ID, stale.LeaseToken); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale complete: got %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, current.ID, current.LeaseToken); err != nil {
		t.Fatalf("current complete: %v", err)
	}
}

// A dead-lettered job that is retried starts again at attempt 1, so attempt
// numbers repeat. A worker still holding a lease from before the dead-letter
// must not be able to complete the new claim.
func TestStaleWorkerCannotOverwriteClaimAfterManualRetry(t *testing.T) {
	s, db := newStore(t)
	ctx := context.Background()
	j := create(t, s, "")
	if _, err := db.Exec(ctx, `UPDATE jobs SET max_attempts = 1 WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}

	stale := claim(t, s)
	expireLeases(t, db)
	if _, err := s.RecoverStale(ctx); err != nil { // last attempt lost -> dead_letter
		t.Fatal(err)
	}
	if _, err := s.RetryDead(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	current := claim(t, s)
	if current.Attempts != stale.Attempts {
		t.Fatalf("precondition: attempts %d and %d should collide", current.Attempts, stale.Attempts)
	}

	if err := s.Complete(ctx, stale.ID, stale.LeaseToken); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale complete: got %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, current.ID, current.LeaseToken); err != nil {
		t.Fatalf("current complete: %v", err)
	}
}

func TestConcurrentMigrationsSucceed(t *testing.T) {
	db := pgtest.New(t)
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			if err := store.New(db).Migrate(context.Background()); err != nil {
				t.Errorf("migrate: %v", err)
			}
		})
	}
	wg.Wait()
}

func TestRetryDeadResetsAttemptBudget(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	create(t, s, "")
	j := claim(t, s)
	if err := s.DeadLetter(ctx, j.ID, j.LeaseToken, "gave up"); err != nil {
		t.Fatal(err)
	}

	got, err := s.RetryDead(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != job.StatusPending || got.Attempts != 0 {
		t.Fatalf("got status %s attempts %d, want pending 0", got.Status, got.Attempts)
	}
}

func TestUnknownOrMalformedIDIsNotFound(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-a-uuid"} {
		if _, err := s.Get(ctx, id); !errors.Is(err, job.ErrNotFound) {
			t.Errorf("Get(%q): got %v, want ErrNotFound", id, err)
		}
		if _, err := s.RetryDead(ctx, id); !errors.Is(err, job.ErrNotFound) {
			t.Errorf("RetryDead(%q): got %v, want ErrNotFound", id, err)
		}
	}
}

// TestStoreEnforcesStateMachine puts a job in every status and runs every
// store operation against it. Each operation implements one edge of the state
// machine and must succeed only from that edge's source status.
func TestStoreEnforcesStateMachine(t *testing.T) {
	s, db := newStore(t)
	ctx := context.Background()

	const token = 1
	ops := []struct {
		name     string
		from, to job.Status
		run      func(id string) error
	}{
		{"claim", job.StatusPending, job.StatusProcessing, func(string) error {
			_, ok, err := s.Claim(ctx, lease)
			if err == nil && !ok {
				return job.ErrInvalidTransition
			}
			return err
		}},
		{"complete", job.StatusProcessing, job.StatusCompleted, func(id string) error { return s.Complete(ctx, id, token) }},
		{"retry", job.StatusProcessing, job.StatusPending, func(id string) error { return s.Retry(ctx, id, token, 0, "x") }},
		{"release", job.StatusProcessing, job.StatusPending, func(id string) error { return s.Release(ctx, id, token) }},
		{"fail", job.StatusProcessing, job.StatusFailed, func(id string) error { return s.Fail(ctx, id, token, "x") }},
		{"dead_letter", job.StatusProcessing, job.StatusDeadLetter, func(id string) error { return s.DeadLetter(ctx, id, token, "x") }},
		{"retry_dead", job.StatusDeadLetter, job.StatusPending, func(id string) error { _, err := s.RetryDead(ctx, id); return err }},
	}
	statuses := []job.Status{job.StatusPending, job.StatusProcessing, job.StatusCompleted, job.StatusFailed, job.StatusDeadLetter}

	// Every operation is a legal edge, and every legal edge has an operation.
	covered := map[[2]job.Status]bool{}
	for _, op := range ops {
		if !job.CanTransition(op.from, op.to) {
			t.Fatalf("operation %s performs illegal transition %s -> %s", op.name, op.from, op.to)
		}
		covered[[2]job.Status{op.from, op.to}] = true
	}
	for _, from := range statuses {
		for _, to := range statuses {
			if job.CanTransition(from, to) && !covered[[2]job.Status{from, to}] {
				t.Errorf("transition %s -> %s has no store operation", from, to)
			}
		}
	}

	for _, from := range statuses {
		for _, op := range ops {
			t.Run(string(from)+"/"+op.name, func(t *testing.T) {
				if _, err := db.Exec(ctx, `TRUNCATE jobs`); err != nil {
					t.Fatal(err)
				}
				j := create(t, s, "")
				if _, err := db.Exec(ctx, `UPDATE jobs SET status = $2, attempts = 1, lease_token = $3 WHERE id = $1`, j.ID, from, token); err != nil {
					t.Fatal(err)
				}

				err := op.run(j.ID)

				if from == op.from {
					if err != nil {
						t.Fatalf("should succeed, got %v", err)
					}
					got, err := s.Get(ctx, j.ID)
					if err != nil {
						t.Fatal(err)
					}
					if got.Status != op.to {
						t.Fatalf("status %s, want %s", got.Status, op.to)
					}
					return
				}
				if !errors.Is(err, job.ErrInvalidTransition) && !errors.Is(err, job.ErrLeaseLost) {
					t.Fatalf("should be rejected, got %v", err)
				}
			})
		}
	}
}
