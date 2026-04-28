-- Run against machines_db/supabase to support owner-based deployment queries.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS username VARCHAR;
