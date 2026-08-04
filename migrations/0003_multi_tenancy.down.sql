ALTER TABLE job_history DROP COLUMN IF EXISTS tenant_id;
ALTER TABLE recurring_jobs DROP COLUMN IF EXISTS tenant_id;
