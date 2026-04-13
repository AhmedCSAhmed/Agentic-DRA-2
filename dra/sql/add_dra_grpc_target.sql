-- Run against machines_db after deploying code that reads dra_grpc_target.
ALTER TABLE machines ADD COLUMN IF NOT EXISTS dra_grpc_target VARCHAR;
