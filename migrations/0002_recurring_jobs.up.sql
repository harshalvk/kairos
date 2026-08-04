CREATE TABLE recurring_jobs (
    id              UUID PRIMARY KEY,
    name            TEXT NOT NULL,
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL,
    cron_expr       TEXT NOT NULL,
    max_attempts    INT NOT NULL DEFAULT 3,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    last_run_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_recurring_jobs_enabled ON recurring_jobs (enabled);
