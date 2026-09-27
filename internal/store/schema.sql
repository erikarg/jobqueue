-- Idempotent schema. Applied on startup by Store.Migrate.
CREATE TABLE IF NOT EXISTS jobs (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    type            text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}',
    status          text        NOT NULL CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'dead_letter')),
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts    integer     NOT NULL CHECK (max_attempts > 0),
    idempotency_key text        UNIQUE,
    run_at          timestamptz NOT NULL DEFAULT now(),
    locked_until    timestamptz,
    lease_token     bigint      NOT NULL DEFAULT 0, -- incremented on every claim, never reset
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz
);

-- Claim query: next due pending job.
CREATE INDEX IF NOT EXISTS jobs_pending_run_at_idx ON jobs (run_at) WHERE status = 'pending';

-- Recovery query: processing jobs whose lease expired.
CREATE INDEX IF NOT EXISTS jobs_processing_locked_until_idx ON jobs (locked_until) WHERE status = 'processing';
