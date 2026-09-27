# jobqueue

An asynchronous job processing service in Go, backed by PostgreSQL.

Clients submit jobs over HTTP; a bounded pool of workers claims them from the
database, runs them with a per-job timeout, retries retryable failures with
exponential backoff, and dead-letters jobs that exhaust their attempts. Jobs
left behind by a crashed process are recovered automatically.

```
HTTP API ──► PostgreSQL (job store) ──► bounded worker pool ──► job handler
```

The API and the worker pool run in the same binary. Any number of instances
can share one database: PostgreSQL row locking is the only coordination.

## Contents

- [Quick start](#quick-start)
- [API](#api)
- [Architecture](#architecture)
- [Job lifecycle](#job-lifecycle)
- [Concurrency model](#concurrency-model)
- [Delivery semantics](#delivery-semantics)
- [Failure handling](#failure-handling)
- [Observability](#observability)
- [Configuration](#configuration)
- [Testing](#testing)
- [Limitations](#limitations)
- [Design decisions](#design-decisions)
- [License](#license)

## Quick start

Requirements: Go 1.25+, Docker.

```sh
make db-up   # PostgreSQL on localhost:5433
make run     # API on :8080, 4 workers
```

The schema is applied on startup. Submit a job:

```sh
curl -s -XPOST localhost:8080/jobs \
  -H 'Idempotency-Key: order-42' \
  -d '{"type":"sleep","payload":{"duration":"2s"}}'

curl -s localhost:8080/jobs/<id>/status
```

Two example handlers are registered in `cmd/jobqueue/handlers.go`:

| Type      | Payload                                  | Behaviour |
|-----------|------------------------------------------|-----------|
| `sleep`   | `{"duration":"2s"}`                      | Waits, honouring cancellation. Invalid duration → permanent failure. |
| `webhook` | `{"url":"https://…","body":{…}}`         | POSTs `body` to `url`. Network error, 429 and 5xx → retry; other 4xx → permanent failure. |

To watch retries and dead-lettering, point a webhook at a closed port:

```sh
curl -s -XPOST localhost:8080/jobs \
  -d '{"type":"webhook","max_attempts":3,"payload":{"url":"http://127.0.0.1:1","body":{}}}'
```

## API

| Method | Path                | Description |
|--------|---------------------|-------------|
| `POST` | `/jobs`             | Create a job. `201` on create, `200` on idempotent replay. |
| `GET`  | `/jobs/{id}`        | Full job record. |
| `GET`  | `/jobs/{id}/status` | Status, attempts, next run time and last error. |
| `POST` | `/jobs/{id}/retry`  | Requeue a dead-lettered job with a fresh attempt budget. `409` for any other status. |
| `GET`  | `/healthz`          | Liveness: the process is serving HTTP. |
| `GET`  | `/readyz`           | Readiness: database reachable and not shutting down. |
| `GET`  | `/metrics`          | Prometheus metrics. |

**Create request**

```json
{ "type": "webhook", "payload": { "url": "…" }, "max_attempts": 5 }
```

`type` must be a registered handler. `payload` is any JSON value (default
`{}`). `max_attempts` is optional (default `MAX_ATTEMPTS`, range 1–100).

**Idempotency.** Send an `Idempotency-Key` header to make submission safe to
retry. The first request with a key creates the job; later requests with the
same key and the same `type` and `payload` return the existing job with `200`.
Reusing a key with a different request returns `409`. Payloads are compared as
`jsonb`, so key order and whitespace don't matter. Keys never expire.

**Errors** are JSON: `{"error": "…"}`. Every response carries an
`X-Request-ID` header (the caller's, if provided, otherwise generated).

## Architecture

```
cmd/jobqueue/        wiring: config, signals, HTTP server, graceful shutdown, example handlers
internal/job/        domain: Job, Status, state machine, error classification
internal/store/      PostgreSQL persistence; every transition is one conditional UPDATE
internal/worker/     bounded worker pool, backoff, lease recovery loop
internal/api/        HTTP handlers, request IDs, access logging, health checks
internal/metrics/    Prometheus metric definitions
internal/pgtest/     isolated per-test database schemas for integration tests
```

Dependencies point one way: `api` and `worker` depend on `job` and `store`
(concretely or through a small interface); `job` depends on nothing. There is
no DI container and no generic repository. The only interface is
`worker.Store`, declared by its consumer so the pool can be unit-tested
without a database.

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending: POST /jobs
    pending --> processing: claimed by a worker
    processing --> completed: handler succeeded
    processing --> pending: retryable error (backoff),<br/>shutdown release, lease expired
    processing --> failed: non-retryable error
    processing --> dead_letter: attempts exhausted
    dead_letter --> pending: POST /jobs/{id}/retry
    completed --> [*]
    failed --> [*]
```

| Status        | Meaning |
|---------------|---------|
| `pending`     | Waiting to run at `run_at` (immediately, or after a backoff delay). |
| `processing`  | Leased by a worker until `locked_until`. |
| `completed`   | Terminal. |
| `failed`      | Terminal. The handler reported a non-retryable error; retrying the same input would fail again. |
| `dead_letter` | Retries exhausted. Can be requeued manually once the cause is fixed. |

The allowed transitions are defined once, in `internal/job/job.go`. They are
enforced by the store: each operation is a single `UPDATE … WHERE status = <from>`,
so a transition either applies atomically or affects zero rows — there is no
read-then-write window for concurrent actors to interleave. A test
(`TestStoreEnforcesStateMachine`) runs every store operation against a job in
every status and checks that the store accepts exactly the transitions in the
state machine.

## Concurrency model

```
                         ┌───────── slots (cap N) ─────────┐
                         │ one token per busy worker       │
                         ▼                                 │
  dispatcher ──take slot──► Claim() ──► jobs (unbuffered) ──► worker 1..N ──► release slot
      │                                                        │
      └─ no job due: release slot, wait PollInterval           └─ run handler with timeout,
                                                                  record outcome
  recovery loop ── every RecoveryInterval ── RecoverStale()
```

- **Bounded.** Exactly `WORKERS` goroutines execute jobs, fed by one
  dispatcher goroutine through an unbuffered channel. No goroutine is created
  per job.
- **Claim only when a worker is free.** The dispatcher takes a slot from a
  buffered channel (a counting semaphore) *before* claiming. A claimed job is
  therefore always handed to a worker immediately; it never sits in an
  in-memory queue while its lease runs down, and nothing is lost from memory
  on a crash.
- **Claiming** uses `SELECT … FOR UPDATE SKIP LOCKED` inside the `UPDATE`.
  Concurrent claimers — goroutines or other processes — skip rows another
  transaction is locking, so they never block each other and never receive
  the same job.
- **Leases and fencing.** A claim sets `locked_until = now() + LEASE_DURATION`
  and increments both `attempts` and `lease_token`. Outcome writes
  (`Complete`, `Retry`, `Fail`, …) require
  `status = 'processing' AND lease_token = <claimed token>`. If a worker's
  lease expires and the job is recovered and re-claimed, the old worker's late
  write matches zero rows and is rejected (`ErrLeaseLost`) instead of
  overwriting the newer claim. `attempts` can't serve as the token: a manual
  retry resets it and a shutdown release decrements it, so attempt numbers
  repeat — `lease_token` only ever grows.
- **Contexts.** Each attempt runs under `context.WithTimeout(JOB_TIMEOUT)`.
  Its parent is an execution context that is *not* cancelled by the shutdown
  signal (`context.WithoutCancel`), so shutdown stops new claims without
  interrupting running jobs. Outcome writes also use a detached context with
  its own timeout, so a result is still recorded while shutting down.
- **Panics** in a handler are recovered, logged with a stack trace, and
  treated as retryable errors. A buggy handler cannot kill a worker.

### Graceful shutdown

On `SIGINT`/`SIGTERM`:

1. The dispatcher stops claiming; the recovery loop stops.
2. `/readyz` starts returning `503`. The server keeps serving for
   `DRAIN_DELAY` so load balancers observe it, then stops accepting
   connections and finishes in-flight requests. A second signal kills the
   process immediately.
3. Running jobs get up to `SHUTDOWN_TIMEOUT` to finish and record their result.
4. Jobs still running after that are cancelled and **released** back to
   `pending` without consuming an attempt (the interruption was not the job's
   fault).
5. The process exits once every worker has returned.

If the process is killed before step 4 completes, lease recovery handles
the job. Handlers must honour `ctx`: one that ignores cancellation blocks
step 5.

## Delivery semantics

**At-least-once.** A handler may run more than once for the same job:

- the process crashes after the handler's side effect but before the result
  is recorded;
- a handler overruns its lease (e.g. ignores context cancellation, or the
  process is paused), the job is recovered and runs again elsewhere.

Exactly-once *execution* is not achievable without the side effect taking
part in the same transaction. Handlers should therefore be idempotent — e.g.
send the job ID as an idempotency key to the downstream service.

What the system does guarantee:

- A job is never *claimed* by two workers at the same time while its lease is
  valid (row locks + status CAS).
- A stale worker can never overwrite the outcome of a newer claim (fencing).
- A submission with an `Idempotency-Key` creates at most one job, including
  under concurrent duplicate requests (unique index + `ON CONFLICT`).
- Jobs are never lost once `POST /jobs` returns: all state lives in
  PostgreSQL, and a claimed-but-abandoned job is recovered.

Ordering is best-effort by `run_at`; there is no FIFO guarantee across
workers or retries.

## Failure handling

**Error classification.** A handler returns `nil` for success. Any other
error is retryable by default. Wrapping it with `job.Permanent(err)` marks it
non-retryable and the job goes straight to `failed`. Classification survives
wrapping (`errors.As`). Unknown job types and invalid payloads are permanent.

**Backoff.** After a retryable error on attempt *n*, the job is rescheduled
after a delay drawn uniformly from `[c/2, c]`, where
`c = min(BACKOFF_BASE × 2^(n-1), BACKOFF_MAX)`. The jitter spreads out jobs that
failed together (e.g. a downstream outage) so they don't retry in lockstep.
The due time is computed by the database clock (`now() + delay`), the same
clock the claim query compares against, so application clock skew doesn't
affect scheduling.

**Timeouts** (context deadline exceeded) are retryable.

**Dead-lettering.** A retryable error on the last attempt (`attempts =
max_attempts`) moves the job to `dead_letter`. `POST /jobs/{id}/retry` resets
`attempts` to 0 and requeues it. `failed` jobs are not retryable through the
API by design: the error said retrying won't help.

**Crash recovery.** A job in `processing` whose `locked_until` has passed
belongs to a worker that died. The recovery loop (on startup and every
`RECOVERY_INTERVAL`) moves such jobs back to `pending`, or to `dead_letter` if
the lost attempt was the last. Because `attempts` is incremented at claim
time, a job that crashes the process every time it runs ("poison pill") still
exhausts its attempts and is dead-lettered instead of crash-looping forever.

## Observability

**Logs** are structured JSON (`log/slog`) on stdout.

- Every HTTP request logs one line with `request_id`, method, path, status
  and duration. Handler logs carry the same `request_id`; job submission logs
  include `job_id`.
- Every job attempt logs `job started` and `job finished` with `job_id`,
  `job_type`, `attempt`, `outcome`, `duration_ms`, the error and the retry
  delay.

**Metrics** (`GET /metrics`, Prometheus format):

| Metric | Type | Description |
|--------|------|-------------|
| `jobs_submitted_total` | counter | Jobs created via the API (replays excluded). |
| `jobs_completed_total` | counter | Successful jobs. |
| `jobs_failed_total{status}` | counter | Jobs that reached `failed` or `dead_letter`. |
| `jobs_retried_total` | counter | Automatic retries scheduled. |
| `jobs_recovered_total` | counter | Jobs recovered from an expired lease. |
| `job_processing_duration_seconds{outcome}` | histogram | Handler execution time. |

Plus the standard Go runtime and process collectors. Counters are
per-process; aggregate across instances in Prometheus.

**Health.** `/healthz` is a liveness probe (no dependencies checked, so a
database outage doesn't cause restarts). `/readyz` pings the database and
fails during shutdown so load balancers drain the instance first.

## Configuration

Environment variables, all optional:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable` | PostgreSQL connection string. |
| `HTTP_ADDR` | `:8080` | Listen address. |
| `WORKERS` | `4` | Concurrent jobs per process. |
| `MAX_ATTEMPTS` | `5` | Default attempt budget per job. |
| `JOB_TIMEOUT` | `30s` | Per-attempt execution limit. |
| `LEASE_DURATION` | `1m` | Claim validity. Must exceed `JOB_TIMEOUT` + 5s (time to record the outcome). |
| `RECOVERY_INTERVAL` | `30s` | How often expired leases are recovered. |
| `POLL_INTERVAL` | `1s` | Wait between claims when no job is due. |
| `BACKOFF_BASE` | `1s` | Delay ceiling after the first failure. |
| `BACKOFF_MAX` | `5m` | Maximum delay ceiling. |
| `SHUTDOWN_TIMEOUT` | `20s` | Grace period for running jobs on shutdown. |
| `DRAIN_DELAY` | `0s` | How long `/readyz` reports `503` before the listener closes. Set it above your load balancer's readiness check interval. |

Recovery of a crashed worker's job takes up to
`LEASE_DURATION + RECOVERY_INTERVAL`.

## Testing

```sh
make test              # unit tests with -race; integration tests are skipped
make db-up
make test-integration  # everything with -race against PostgreSQL
```

Integration tests read `TEST_DATABASE_URL`. Each test runs in its own
temporary PostgreSQL schema, so packages run in parallel against one database
without interfering.

| Area | Tests |
|------|-------|
| State machine | Allowed transitions; the store accepts exactly those transitions for every status × operation. |
| Processing | Success, retry-until-success with backoff bounds, dead-lettering on the last attempt, permanent errors, unknown types, panics, per-job timeout. |
| Backoff | Exponential growth, cap, jitter bounds, no overflow. |
| Concurrency (`-race`) | Pool never exceeds `WORKERS`; concurrent claims never return the same job; three pools on one database execute 200 jobs exactly once each; concurrent duplicate submissions create one job. |
| Reliability | Graceful shutdown finishes in-flight jobs and stops claiming; shutdown timeout releases jobs without consuming attempts; expired leases are recovered or dead-lettered; a stale worker cannot overwrite a newer claim, including after a manual retry resets attempts; concurrent migrations. |
| API | End-to-end submit → complete; idempotent replay and conflicting key; dead-letter → manual retry → complete; `409`/`404`/`400` paths; health, readiness and request IDs. |

The worker pool's unit tests use an in-memory fake of `worker.Store`; the
store and API tests use a real database, because the correctness of this
system lives in its SQL.

## Limitations

These are deliberate trade-offs for the current scope:

- **Polling.** Idle workers poll every `POLL_INTERVAL`, which bounds pickup
  latency and adds a small constant query load. `LISTEN/NOTIFY` would lower
  latency but adds reconnection handling.
- **Single table.** Completed jobs are never deleted; a production deployment
  needs a retention job (or partitioning by `created_at`). Idempotency keys
  are kept forever along with their jobs.
- **Throughput.** Not benchmarked. Every state change is a row update, so at
  high volume MVCC churn and vacuum become the bottleneck; at that point a
  dedicated broker becomes worth its operational cost.
- **Handlers must honour `ctx`.** Go cannot kill a goroutine. A handler that
  ignores cancellation delays shutdown and can outlive its lease, leading to
  a duplicate run.
- **No priorities, scheduling API, per-type concurrency limits or rate limits.**
- **Schema management.** `schema.sql` is idempotent (`IF NOT EXISTS`) and
  applied on startup under an advisory lock, so instances starting together
  don't race. Evolving it would call for a migration tool.
- **Idempotent replay returns the current state** of the job (which may have
  progressed), not a byte-for-byte copy of the original response.
- **The `webhook` example handler calls arbitrary URLs.** Fine for a demo; a
  real service would restrict destinations to avoid SSRF.
- **No authentication** — out of scope.

## Design decisions

- **PostgreSQL as the queue, no broker.** The job store must be durable and
  queryable (status, retry) anyway. Using it as the queue as well removes
  dual writes between a database and a broker and the consistency problems
  they bring. `FOR UPDATE SKIP LOCKED` is exactly the primitive a work queue
  needs.
- **Leases instead of long-held locks.** Holding a row lock (open
  transaction) for the length of a job would tie up a connection per running
  job and hide in-flight work from status queries. A lease is a plain column:
  visible, cheap, and recoverable.
- **One conditional `UPDATE` per transition** instead of
  read-validate-write in Go. It is atomic without explicit transactions and
  stays correct across processes.
- **Attempts counted at claim time**, so crashes consume attempts and poison
  pills terminate.
- **Shutdown interruptions don't count as attempts**, so frequent deploys
  can't dead-letter healthy jobs.
- **`pgx` directly, no ORM.** The queries are few and the SQL is the point.
- **Standard library HTTP router** (Go 1.22+ method and path patterns); no
  web framework is needed for seven routes.
- **Prometheus client** rather than a hand-rolled exposition format: it is
  the de facto standard and provides correct histograms.

## License

[MIT](LICENSE)
