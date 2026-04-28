-- Run against machines_db/supabase to require password-protected username lookups.

CREATE TABLE IF NOT EXISTS deployment_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR NOT NULL UNIQUE,
    password_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS user_id INTEGER;

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_user_id_fkey;
ALTER TABLE jobs
    ADD CONSTRAINT jobs_user_id_fkey
    FOREIGN KEY (user_id)
    REFERENCES deployment_users(id)
    ON DELETE SET NULL;
