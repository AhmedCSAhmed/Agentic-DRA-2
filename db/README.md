Postgres seeding lives in [db/init/001_shared_seed.sql](/Users/castro/Desktop/Agentic-DRA-2/db/init/001_shared_seed.sql).

Workflow:

1. Export the current database state:
   `docker exec agentic-postgres pg_dump -U postgres machines_db > db/init/001_shared_seed.sql`
2. Commit the updated seed file and [docker-compose.yml](/Users/castro/Desktop/Agentic-DRA-2/docker-compose.yml).
3. Teammates recreate their local Postgres volume:
   `docker compose down -v`
4. Teammates start Postgres again:
   `docker compose up -d`

Important:

- The seed only loads on first initialization of the Postgres data directory.
- Existing Docker volumes will not be overwritten automatically.
