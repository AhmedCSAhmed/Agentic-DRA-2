# Atlas — Distributed Resource Allocator (DRA)

Atlas is a lightweight tool for deploying Docker workloads across a handful of
machines. You ask it to run an image with some resource requirements, and Atlas
picks a machine that has room, launches the container there over gRPC, enforces
the resource limits, and tracks what's running so you can list and stop it later.

It's built from a few simple parts: a Postgres-backed machine registry, a
resource-aware placement step (filter machines by free memory/cores and
heartbeat freshness, then pick the best), and a small gRPC agent on each host
that does the actual `docker pull` / `docker run`.

## What it accomplishes

- **Turns a pile of machines into a schedulable pool.** Each host registers in
  Postgres with its gRPC endpoint and advertises how much memory and how many
  CPU cores it has free. Atlas treats the whole fleet as one resource pool.
- **Places workloads intelligently.** Given a request like "run `nginx:latest`,
  needs 2 GB", the scheduler filters out machines that are the wrong type, too
  full, or stale (no recent heartbeat), then picks the best remaining host —
  preferring remote machines and the one with the most free memory.
- **Runs containers remotely over gRPC.** The chosen host's DRA agent pulls the
  image and runs it detached, applying real `--memory` / `--cpus` limits that
  match the reservation so a workload can't quietly exceed what it asked for.
- **Keeps capacity accounting correct.** Reservations are decremented atomically
  when a container starts and released when it stops — whether it's stopped
  explicitly, exits on its own, or the host reconciles against real Docker state.
- **Fails gracefully.** When nothing in the fleet can satisfy a request, callers
  get a typed `NO_CAPACITY` response (with counts and reject reasons) instead of
  an error, so clients can retry or lower their requirements.
- **Meets you where you are.** The same scheduling-and-deploy core is reachable
  three ways: an interactive CLI, an HTTP API, and an LLM agent.

## Interfaces

All three entry points share one scheduler and one gRPC deploy path
(`scheduled_deploy.py`):

- **`atlas` CLI / REPL** — an interactive terminal client (`deploy`, `stop`, `status`, `instances`).
- **HTTP API** — a FastAPI service exposing `POST /deploy`.
- **Agent** — an OpenAI-Agents agent that lists machines from the registry and calls the deploy RPC.

## How a deploy flows

1. A client (CLI, HTTP, or agent) submits an image plus `resource_requirements`
   (memory, optional cores) and optional filters (machine type, command,
   restart policy).
2. The scheduler loads candidate machines from Postgres and **filters** them —
   dropping wrong types, machines without enough free memory/cores, and hosts
   whose heartbeat is stale.
3. Survivors are **ranked** (remote hosts preferred, then most free memory) and
   the best is selected. If none survive → typed `NO_CAPACITY`.
4. Atlas invokes `PullAndRunImage` over gRPC on the selected host. The DRA agent
   there reserves capacity in Postgres, `docker pull`s if needed, and
   `docker run`s the container detached with enforced resource limits.
5. A **job record** is written to Postgres (owner, machine, reservation) so the
   container can be listed and later stopped.
6. The host keeps the ledger accurate over time: it **heartbeats**, watches the
   container, and **releases** the reserved memory/cores when it exits — on
   explicit `stop`, on natural exit, or via periodic reconciliation against
   `docker inspect`.

---

## Architecture

```
              ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
              │  atlas CLI   │     │  HTTP API    │     │    Agent     │
              │  (cli/)      │     │  (routes/)   │     │  (agent/)    │
              └──────┬───────┘     └──────┬───────┘     └──────┬───────┘
                     └────────────────────┼────────────────────┘
                                          │
                            scheduled_deploy.py
                     (scheduler + remote deploy, shared)
                                          │
                    ┌─────────────────────┼──────────────────────┐
                    │                                             │
            select best machine                          gRPC PullAndRunImage
            (routes/scheduler.py)                        (agent/client.py)
                    │                                             │
                    ▼                                             ▼
            ┌───────────────┐                          ┌────────────────────┐
            │  Postgres     │  machines / jobs /       │  DRA gRPC server   │
            │  registry     │  users / job_queue       │  (dra/) on a host  │
            └───────────────┘                          │  runs docker pull/ │
                                                       │  run + heartbeats  │
                                                       └────────────────────┘
```

The **DRA gRPC server** (`dra/`) runs on each worker machine. It executes
containers, enforces `--memory` / `--cpus` limits matching the reservation,
sends heartbeats, reconciles running jobs with real Docker state, and releases
reserved capacity when a container exits.

The **scheduler** (`routes/scheduler.py`) filters machines by type, available
memory/cores, and heartbeat freshness, prefers non-loopback (remote) hosts, and
ranks by most available memory. If no machine qualifies it returns a typed
`NO_CAPACITY` result instead of erroring.

---

## Components

| Path | Role |
|------|------|
| `dra/` | DRA gRPC server: `PullAndRunImage`, `StopContainer`, capacity bookkeeping, heartbeats. Run with `python -m dra`. |
| `dra/grpc_server.py` | Servicer that runs Docker and maintains machine/job accounting in Postgres. |
| `dra/repositories/` | Data access for `machines`, `jobs`, `job_queue`, `deployment_users`. |
| `dra/models.py` | SQLAlchemy ORM models for the Postgres schema. |
| `agent/` | OpenAI-Agents agent + gRPC client and tools (`list_dra_machines`, `pull_and_run_image`, `start_dra_grpc_server`). |
| `routes/` | FastAPI app (`POST /deploy`), request/response contracts, scheduler, and an optional LLM-based batch scheduler. |
| `scheduled_deploy.py` | Shared "schedule → deploy over gRPC → persist job" flow used by both the API and the CLI. |
| `cli/`, `atlas_cli.py` | `atlas` Typer CLI and interactive REPL. |
| `dra.proto`, `dra_pb2.py`, `dra_pb2_grpc.py` | gRPC service definition and generated stubs. |
| `tests/` | Pytest suite (gRPC, scheduler, repositories, deploy path). |
| `docker-compose.yml` | Compose services for the DRA server, the agent, and an optional local Postgres. |

### gRPC service (`dra.DRAService`)

Defined in `dra.proto`. Two unary RPCs.

**`PullAndRunImage(PullAndRunRequest) → PullAndRunResponse`** — pulls the image
if it isn't already local, then runs it detached and returns runtime metrics.

`PullAndRunRequest` fields:

| Field | Type | Meaning |
|-------|------|---------|
| `image_name` | string | Docker image reference (validated against `^[a-zA-Z0-9][a-zA-Z0-9._/\-:@]{0,254}$`). |
| `command` | repeated string | Args appended after the image (the container CMD), e.g. `["sleep", "infinity"]`. Use when the image's default command exits immediately. |
| `restart_policy` | string | Docker `--restart` value: `no`, `on-failure`, `always`, `unless-stopped`, or `on-failure:N`. |
| `memory_gb` | float | Memory to reserve. Decrements `machines.available_gb` and is enforced with `--memory` / `--memory-swap`. |
| `cpu_cores` | float | Cores to reserve. Decrements `machines.available_cores` and is enforced with `--cpus`. |

`PullAndRunResponse` fields: `success` (bool), `container_id` (string),
`workload_state` (`RUNNING` / `ERROR` / …), `cpu_used` (float),
`memory_gb_used` (float), `message` (string).

The servicer validates input, reserves capacity in Postgres *before* starting
the container, and rolls the reservation back if the pull or run fails. On
success it records a job row and starts a background watcher that releases
capacity when the container later exits.

**`StopContainer(StopContainerRequest) → StopContainerResponse`** — force-removes
the container (`docker rm -f`) and releases its reserved memory/cores back to the
machine. The release is idempotent: capacity is only returned if the job was
still `RUNNING`, so an explicit stop racing with the exit watcher can't
double-credit the machine. Returns `success`, `message`, and `memory_gb_released`.

---

## Data model

Atlas keeps all cluster state in Postgres. The tables are defined as SQLAlchemy
models in `dra/models.py`.

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `machines` | The registry — one row per host in the fleet. | `machine_id` (PK), `machine_name`, `machine_type`, `dra_grpc_target` (`host:port`), `available_gb`, `available_cores`, `last_heartbeat_at` |
| `jobs` | One row per running/finished deployment, for listing and stop/release bookkeeping. | `id` (PK), `image_id` (container id), `image_name`, `status`, `resource_requirements` (JSONB: memory, cores, machine_id, owner), `username`, `user_id` |
| `deployment_users` | Owners of deployments; supports the CLI's `--username` / `--password`. | `id` (PK), `username` (unique), `password_hash` |
| `job_queue` | Pending work for the optional batch/LLM scheduler. | `id` (PK), `image_name`, `resource_requirements`, `machine_type`, `status`, `scheduled_for`, `batch_id`, `machine_id`, `decision_reason` |
| `scheduler_decisions` | Audit trail of batch scheduling decisions (dispatch / delay / batch). | `id` (PK), `job_queue_ids`, `action`, `machine_id`, `reason`, `mode`, `decided_at` |

**Capacity model.** `available_gb` and `available_cores` are the source of truth
for how much room a machine has. They're seeded from the host's real CPU/RAM
when a DRA server starts with `--machine-name`, then adjusted by *atomic*
increments (`SELECT … FOR UPDATE`) on every reserve and release so concurrent
deploys to the same machine can't corrupt the count. `last_heartbeat_at` lets
the scheduler ignore machines that have gone silent.

---

## Scheduling in detail

The placement logic lives in `routes/scheduler.py` (`rank_eligible_machines` /
`select_best_machine`). Given the registered machines and a request's
`resource_requirements`, it runs two phases:

**1. Filter (hard constraints).** A machine is dropped — and the reason counted —
when any of these hold:

- `machine_type_mismatch` — a `machine_type` filter was given and the machine is a different type.
- `insufficient_memory` — `available_gb` is below the requested `memory_gb`.
- `insufficient_cpu` — `cpu_cores` was requested and `available_cores` is below it.
- `stale_telemetry` — `last_heartbeat_at` is older than `DRA_SCHEDULER_HEARTBEAT_STALE_SECONDS` (default 120s).

Machines with no `dra_grpc_target` are excluded before this stage (they can't be
reached). Machines that are missing telemetry entirely fall back to assumed
capacity (`DRA_SCHEDULER_FALLBACK_AVAILABLE_GB` / `_CORES`) so a freshly
registered host isn't silently dropped.

**2. Rank (pick the best survivor).**

- If `DRA_SCHEDULER_PREFER_REMOTE` is on (default) and any non-loopback machine
  is eligible, loopback (`127.0.0.1` / `localhost`) machines are set aside so
  real cluster hosts win over the local one.
- Remaining machines are sorted by **most `available_gb` first**, with
  `machine_id` ascending as a deterministic tie-breaker.

If nothing survives the filter, the caller gets a `SchedulerDecision` with
`selected = None`, the scanned/eligible counts, and the aggregated reject
reasons — which the HTTP layer turns into a `NO_CAPACITY` response.

During an actual deploy (`scheduled_deploy.py`), Atlas walks the ranked list and
**fails over**: if a host returns a retryable gRPC error (`UNAVAILABLE` /
`DEADLINE_EXCEEDED`), it tries the next-best machine before giving up.

There is also an optional **LLM batch scheduler** (`routes/llm_scheduler.py`)
that reasons over the `job_queue` table and emits `dispatch` / `delay` / `batch`
decisions, recorded in `scheduler_decisions`. The synchronous per-request path
above is the default; the LLM scheduler is for batching queued work.

---

## Using Atlas across HPC nodes

Atlas maps naturally onto a compute cluster: every node becomes a schedulable
member of one pool, and containerized jobs are placed onto whichever node has
the memory and cores to run them.

**How the pieces line up.**

- **One DRA agent per node.** Run `python -m dra --machine-name <node>` on each
  compute node. It seeds `available_cores` / `available_gb` from that node's real
  CPU and RAM, heartbeats to Postgres, and executes containers locally. A login
  or head node runs the CLI / HTTP API and does the placement.
- **Node classes via `machine_type`.** Register nodes with a `machine_type` such
  as `cpu`, `highmem`, or `gpu`, and pin a job to a class with
  `--machine-type` / the request's `machine_type` field. The scheduler filters
  to matching nodes before ranking.
- **Reservations map to real limits.** `--memory-gb` and `--cpu-cores` reserve
  capacity in the registry *and* are enforced on the node via Docker's
  `--memory` / `--cpus` (Linux cgroups), so a job can't exceed the slice it was
  scheduled for — a rough analogue to a batch scheduler's resource requests.
- **Capacity stays honest under churn.** Atomic reserve/release plus the
  container watcher and Docker reconciliation keep `available_*` accurate as many
  short jobs start and finish across the fleet — useful for high-throughput,
  many-task workloads.
- **Retry and fail-over.** If a node's agent is unreachable, the deploy path
  automatically tries the next-best node, which suits clusters where individual
  nodes drain or reboot.

**Example: a small heterogeneous cluster.**

```bash
# on each node (systemd unit, tmux, etc.)
DATABASE_URL=postgresql+psycopg://…  python -m dra --machine-name gpu-node-01
DATABASE_URL=postgresql+psycopg://…  python -m dra --machine-name highmem-node-02

# from the head node — place a memory-heavy job on the highmem class
atlas deploy my/simulation:latest --machine-type highmem --memory-gb 128 --cpu-cores 16
```

**Scope and caveats.** Atlas is a lightweight, Docker/cgroup-based placer, not a
replacement for a full HPC batch system. It has no gang scheduling, no MPI /
multi-node job coordination, no interconnect- or NUMA-topology awareness, no
queue fair-share, and it targets Docker rather than Slurm or Apptainer/Singularity.
It fits best for **containerized, single-node throughput jobs** (services, batch
tasks, per-node compute) on nodes where you control the Docker daemon — or as a
simple self-hosted layer in front of a pool of GPU/CPU boxes. For tightly-coupled
parallel jobs, keep using your existing scheduler.

---

## HTTP API reference

`routes/main.py` mounts a FastAPI app with a single endpoint.

### `POST /deploy`

Request body (`routes/contracts.py::DeployRequest`):

```json
{
  "image_name": "nginx:latest",
  "resource_requirements": { "memory_gb": 2, "cpu_cores": 1 },
  "command": "sleep infinity",
  "restart_policy": "unless-stopped",
  "machine_type": "cpu",
  "username": "alice",
  "password": "…",
  "request_id": "optional-client-id"
}
```

Only `image_name` and `resource_requirements.memory_gb` are required
(`memory_gb` and `cpu_cores` must be `> 0`).

Success (`200`):

```json
{
  "status": "DEPLOYED",
  "request_id": "req-…",
  "selected_machine": { "machine_id": "…", "machine_type": "cpu", "grpc_target": "10.0.0.5:50051" },
  "container": { "container_id": "…", "workload_state": "RUNNING" },
  "metrics": { "cpu_used": 0.0, "memory_gb_used": 0.0 },
  "message": "Image pulled and container started"
}
```

Failures return `{ "status": "FAILED", "error_code": …, "retryable": …, "details": … }`
with these codes and HTTP statuses:

| `error_code` | HTTP | Retryable | When |
|--------------|------|-----------|------|
| `NO_CAPACITY` | 409 | yes | No machine satisfied the request after filtering. `details` carries `considered_machines` and `reject_reasons`. |
| `GRPC_UNAVAILABLE` | 503 | yes | The selected host's gRPC endpoint was unreachable or timed out. |
| `REMOTE_EXECUTION_FAILED` | 502 | no | The host was reached but the pull/run (or tool invocation) failed. |

---

## Requirements

- Python 3.11+
- Docker (on each machine that runs the DRA server)
- A Postgres database (e.g. Supabase) reachable via `DATABASE_URL`
- An OpenAI API key if you use the agent or the LLM scheduler

---

## Setup

```bash
# from the repo root
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .        # installs the `atlas` command (console script)
```

Create a `.env` file in the repo root:

```dotenv
DATABASE_URL=postgresql+psycopg://USER:PASSWORD@HOST:5432/DBNAME
OPENAI_API_KEY=sk-...           # for the agent / LLM scheduler
ATLAS_ADMIN_PASSCODE=...        # required for `atlas --admin`
```

The database must contain the tables defined in `dra/models.py`
(`machines`, `jobs`, `deployment_users`, `job_queue`, `scheduler_decisions`).
Each machine you want to schedule onto needs a row in `machines` with a valid
`dra_grpc_target` (`host:port`).

---

## Running

### 1. Start a DRA server on each worker machine

```bash
# uses DRA_GRPC_BIND (default 0.0.0.0:50051)
python -m dra

# tie this host to a registry row so it heartbeats and reconciles capacity
python -m dra --machine-name worker-east-01
```

When started with `--machine-name`, the server loads that `machines` row, seeds
`available_cores` / `available_gb` from the host, sends heartbeats, and
auto-releases reserved capacity when containers exit.

### 2a. Deploy with the `atlas` CLI

```bash
atlas                              # interactive REPL
atlas --admin                      # admin REPL (needs ATLAS_ADMIN_PASSCODE)

atlas deploy nginx:latest --memory-gb 2 --restart-policy unless-stopped
atlas status                       # list machines
atlas instances                    # list deployments
atlas stop <container_id>          # stop + release capacity
```

REPL commands: `deploy`, `stop` / `off`, `status`, `instances`, `help`, `q`.

### 2b. Deploy over HTTP

```bash
uvicorn routes.main:app --reload
```

```bash
curl -X POST localhost:8000/deploy \
  -H 'content-type: application/json' \
  -d '{
        "image_name": "nginx:latest",
        "resource_requirements": {"memory_gb": 2},
        "restart_policy": "unless-stopped"
      }'
```

The scheduler picks the best machine, runs the image on it over gRPC, and
persists a job. If nothing has capacity it returns HTTP `409` with a
`NO_CAPACITY` error body.

### 2c. Deploy with the agent

```bash
python -m agent nginx:latest
```

The agent lists machines from the registry, chooses a host, and calls
`pull_and_run_image` over gRPC.

### With Docker Compose

```bash
export DATABASE_URL=postgresql+psycopg://...   # Supabase direct URL
docker compose up -d dra                        # start the DRA server
docker compose run --rm agent nginx:latest      # run the agent once
docker compose --profile local-db up -d postgres  # optional local Postgres
```

---

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | — | Postgres connection (SQLAlchemy/psycopg). Required. |
| `OPENAI_API_KEY` | — | Agent and LLM scheduler. |
| `ATLAS_ADMIN_PASSCODE` | — | Required for `atlas --admin`. |
| `DRA_GRPC_BIND` | `0.0.0.0:50051` | DRA server listen address. |
| `DRA_GRPC_MAX_WORKERS` | `10` | DRA server thread pool size. |
| `DRA_GRPC_TARGET` | — | Default gRPC target the agent connects to. |
| `DRA_MACHINE_NAME` | — | Same as `--machine-name`; ties the server to a registry row. |
| `DRA_HOST_CORES` / `DRA_HOST_MEMORY_GB` | auto-detected | Override detected host capacity. |
| `DRA_DOCKER_RESTART_POLICY` | — | Default `docker run --restart` value. |
| `DRA_RUN_COMMAND` | — | Default container command args. |
| `DRA_HEARTBEAT_INTERVAL_SECONDS` | `30` | Heartbeat cadence. |
| `DRA_CONTAINER_POLL_INTERVAL_SECONDS` | `30` | Container-exit watcher interval. |
| `DRA_SCHEDULER_HEARTBEAT_STALE_SECONDS` | `120` | Drop machines whose heartbeat is older than this. |
| `DRA_SCHEDULER_PREFER_REMOTE` | `1` | Prefer non-loopback machines when scheduling. |
| `DRA_SCHEDULER_FALLBACK_AVAILABLE_GB` / `_CORES` | `64.0` / `8.0` | Assumed capacity when a row lacks telemetry. |

---

## Testing

```bash
source venv/bin/activate
pytest
```

---

## Regenerating gRPC stubs

If you change `dra.proto`, regenerate the Python stubs:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dra.proto
```
