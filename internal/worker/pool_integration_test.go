package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/erikarg/jobqueue/internal/job"
	"github.com/erikarg/jobqueue/internal/metrics"
	"github.com/erikarg/jobqueue/internal/pgtest"
	"github.com/erikarg/jobqueue/internal/store"
	"github.com/erikarg/jobqueue/internal/worker"
)

// TestMultiplePoolsProcessEachJobExactlyOnce simulates several service
// instances sharing one database and checks that no job runs twice.
func TestMultiplePoolsProcessEachJobExactlyOnce(t *testing.T) {
	db := pgtest.New(t)
	st := store.New(db)
	ctx := context.Background()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	const jobs, pools = 200, 3
	for range jobs {
		if _, _, err := st.Create(ctx, store.NewJob{Type: "count", Payload: json.RawMessage(`{}`), MaxAttempts: 3}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu   sync.Mutex
		runs = map[string]int{}
	)
	handlers := map[string]job.Handler{
		"count": func(context.Context, json.RawMessage) error { return nil },
	}
	// Handlers don't see job IDs, so executions are counted at claim time:
	// every successful claim runs the handler exactly once.
	counting := &countingStore{Store: st, onClaim: func(id string) {
		mu.Lock()
		runs[id]++
		mu.Unlock()
	}}

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range pools {
		p, err := worker.New(counting, handlers, worker.Config{
			Workers: 4, PollInterval: 5 * time.Millisecond, JobTimeout: time.Second,
			LeaseDuration: 10 * time.Second, RecoveryInterval: time.Second, ShutdownTimeout: time.Second,
			BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
		}, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New(prometheus.NewRegistry()))
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() { p.Run(runCtx) })
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		var remaining int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status <> 'completed'`).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d jobs not completed", remaining)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if len(runs) != jobs {
		t.Fatalf("%d distinct jobs executed, want %d", len(runs), jobs)
	}
	for id, n := range runs {
		if n != 1 {
			t.Errorf("job %s executed %d times", id, n)
		}
	}
}

type countingStore struct {
	*store.Store
	onClaim func(id string)
}

func (c *countingStore) Claim(ctx context.Context, lease time.Duration) (job.Job, bool, error) {
	j, ok, err := c.Store.Claim(ctx, lease)
	if ok {
		c.onClaim(j.ID)
	}
	return j, ok, err
}
