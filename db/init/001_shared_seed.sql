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
    resource_requirements JSONB NOT NULL,
    image_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

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
