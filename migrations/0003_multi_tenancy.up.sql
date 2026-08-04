ALTER TABLE job_history ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE recurring_jobs ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX idx_job_history_tenant ON job_history (tenant_id);
CREATE INDEX idx_recurring_jobs_tenant ON recurring_jobs (tenant_id);
