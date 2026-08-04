CREATE TABLE job_history (
    id              UUID PRIMARY KEY,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_job_history_status ON job_history (status);
CREATE INDEX idx_job_history_type ON job_history (type);