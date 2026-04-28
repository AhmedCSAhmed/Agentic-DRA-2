-- machines_db schema + seed
--
-- ALWAYS apply after clone or if \dt shows no tables (old volume skipped init):
--   bash scripts/db_apply.sh
--
-- Fresh volume: also runs automatically from ./db/init -> /docker-entrypoint-initdb.d
--
-- Dump from running container to replace this file:
--   bash scripts/dump_machines_db_to_init.sh

CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    image_id VARCHAR NOT NULL,
    username VARCHAR,
    user_id INTEGER,
    resource_requirements JSONB NOT NULL,
    image_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE IF EXISTS jobs
    ADD COLUMN IF NOT EXISTS username VARCHAR;
ALTER TABLE IF EXISTS jobs
    ADD COLUMN IF NOT EXISTS user_id INTEGER;

CREATE TABLE IF NOT EXISTS deployment_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR NOT NULL UNIQUE,
    password_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE IF EXISTS jobs
    DROP CONSTRAINT IF EXISTS jobs_user_id_fkey;
ALTER TABLE IF EXISTS jobs
    ADD CONSTRAINT jobs_user_id_fkey
    FOREIGN KEY (user_id)
    REFERENCES deployment_users(id)
    ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS machines (
    machine_id VARCHAR PRIMARY KEY,
    machine_name VARCHAR NOT NULL,
    machine_type VARCHAR NOT NULL,
    machine_created_at TIMESTAMPTZ NOT NULL,
    machine_updated_at TIMESTAMPTZ NOT NULL,
    dra_grpc_target VARCHAR,
    available_gb DOUBLE PRECISION,
    available_cores DOUBLE PRECISION,
    last_heartbeat_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS job_queue (
    id              SERIAL PRIMARY KEY,
    image_name      VARCHAR NOT NULL,
    resource_requirements JSONB NOT NULL,
    machine_type    VARCHAR,
    command         VARCHAR,
    restart_policy  VARCHAR,
    status          VARCHAR NOT NULL DEFAULT 'PENDING',
    scheduled_for   TIMESTAMPTZ,
    batch_id        VARCHAR,
    container_id    VARCHAR,
    machine_id      VARCHAR,
    decision_reason TEXT,
    decision_mode   VARCHAR,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS scheduler_decisions (
    id              SERIAL PRIMARY KEY,
    job_queue_ids   INTEGER[] NOT NULL,
    action          VARCHAR NOT NULL,
    machine_id      VARCHAR,
    delay_seconds   INTEGER,
    batch_id        VARCHAR,
    reason          TEXT NOT NULL,
    mode            VARCHAR NOT NULL,
    decided_at      TIMESTAMPTZ NOT NULL
);

-- Optional example row (safe to delete)
INSERT INTO machines (
    machine_id,
    machine_name,
    machine_type,
    machine_created_at,
    machine_updated_at,
    dra_grpc_target,
    available_gb,
    available_cores,
    last_heartbeat_at
)
VALUES (
    'local-1',
    'local-dra',
    'cpu',
    NOW(),
    NOW(),
    '127.0.0.1:50051',
    16.0,
    NULL,
    NULL
)
ON CONFLICT (machine_id) DO NOTHING;
