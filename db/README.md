Supabase migration assets live in [db/supabase/schema.sql](/Users/castro/Desktop/Agentic-DRA-2/db/supabase/schema.sql).

Recommended migration flow:

1. Create a Supabase project.
2. In Supabase, open `Project Settings -> Database` and copy the Postgres connection string.
3. Add that connection string to a local `.env` file as `DATABASE_URL`.
   Use [`.env.example`](/Users/castro/Desktop/Agentic-DRA-2/.env.example) as the template.
4. In the Supabase SQL editor, run [db/supabase/schema.sql](/Users/castro/Desktop/Agentic-DRA-2/db/supabase/schema.sql).
5. Start the app. `dra/database.py` now reads `DATABASE_URL` automatically.

Notes:

- The application talks to Supabase over the normal Postgres protocol via SQLAlchemy, so no repository changes are required.
- `DATABASE_URL` is now required; there is no localhost Docker fallback anymore.
- If you want to manage schema changes over time, promote `db/supabase/schema.sql` into versioned SQL migrations in your preferred workflow.
