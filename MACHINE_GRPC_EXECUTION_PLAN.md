Machine gRPC Connection + Remote Docker Execution Plan

Goal
- Connect the agent to a selected machine's gRPC endpoint from the `machines` schema.
- Execute `PullAndRunImage` on that machine via `dra.DRAService`.

Scope (no code yet)
- Define the control flow and key decisions.
- Align data contracts and failure handling.
- Agree on verification steps before implementation.

Current Components
- `machines` table stores machine metadata and optional `dra_grpc_target`.
- Agent tool `pull_and_run_image` can resolve `grpc_target` from `machine_id`.
- gRPC client supports per-call target override.
- DRA server exposes RPC `PullAndRunImage`.

End-to-End Flow (target behavior)
1) User requests deployment with image details and resource requirements.
2) Agent resolves target machine:
   - Always select the best available machine from `machines` using scheduler logic.
3) Agent resolves machine endpoint (`dra_grpc_target` as host:port).
4) Agent invokes gRPC `PullAndRunImage` on that endpoint.
5) Remote DRA server runs docker pull/run and returns response.
6) Agent returns deployment result (container id, state, metrics, message).

Scheduler Contract Draft (no code)
- Inputs:
  - `resource_requirements`: requested minimums (initially memory GB required; CPU optional for v1).
  - `machines`: rows from DB with `machine_id`, `machine_type`, `dra_grpc_target`.
  - `availability_map`: runtime telemetry keyed by `machine_id` (e.g., `available_gb`, optional health/latency).
- Hard filters (exclude before scoring):
  - Machine row exists in DB.
  - `dra_grpc_target` is present and valid `host:port`.
  - Telemetry exists and is fresh (recommended max age: 30s).
  - Machine is reachable/healthy at gRPC layer.
  - Available capacity meets requested minimums.
- Scoring:
  - Primary: highest `available_gb`.
  - Secondary (optional v1.1): lower latency / higher free CPU.
  - Tie-breaker: deterministic by `machine_id` ascending.
- Output:
  - Success: `{ selected_machine_id, grpc_target, selection_reason, considered_count }`.
  - Failure: typed no-capacity result (not an exception leak to caller).

Graceful No-Capacity Behavior
- Condition: all machines filtered out OR none meet requested minimums.
- Return a structured, user-safe response:
  - `error_code`: `NO_CAPACITY`
  - `message`: clear explanation that no machine currently satisfies requirements
  - `requested`: echo resource requirements
  - `considered_machines`: total scanned and eligible counts
  - `retry_hint`: short suggestion (retry later or lower requirements)
- Do not call `PullAndRunImage` when no-capacity is detected.
- Record as a non-terminal scheduling failure in job history for auditability.

Design Decisions to Confirm
- Source of truth for target endpoint: DB `machines.dra_grpc_target` only.
- Best-available selection strategy (inputs, scoring, tie-breakers, and exclusion rules).
- Retry policy for transient gRPC failures.
- Timeout values for RPC and Docker operations.
- What to persist as a job record after call completion.

Validation Rules
- `dra_grpc_target` must be valid `host:port`.
- `image_name` must pass server-side validation.
- Optional `command` and `restart_policy` must be sanitized/validated.
- `resource_requirements` must be present and numeric where applicable (no negative values).

Error Handling Matrix
- Machine missing in DB -> user-facing not-found error.
- Machine exists but no endpoint -> configuration error.
- gRPC UNAVAILABLE/DEADLINE_EXCEEDED -> network/host error (retry candidate).
- RPC internal/docker failure -> execution error with server message.
- Invalid arguments -> fast fail with actionable message.
- No machine meets requirements -> graceful `NO_CAPACITY` response (no RPC call).

Observability
- Log correlation fields: `request_id`, `machine_id`, `grpc_target`, `image_name`.
- Capture RPC outcome and latency.
- Capture scheduler diagnostics: scanned count, filtered count, reject reasons.

Implementation Sequence (next)
1) Finalize best-available scoring contract.
2) Finalize request/response contract for deployment entrypoint.
3) Wire repository selection -> gRPC target resolution.
4) Wire RPC call -> result mapping -> job persistence.
5) Add integration tests across DB + gRPC happy/error paths.

Definition of Done
- Deploy always selects the best available machine automatically.
- Deploy returns graceful typed `NO_CAPACITY` when capacity is insufficient.
- Clear failures for machine availability, endpoint config, and gRPC execution errors.
- Tests demonstrate one successful remote run and representative failures.

Deployment API Contract Draft (no code)
- Endpoint:
  - `POST /deploy`
- Purpose:
  - Accept a deployment request, schedule to best available machine, execute gRPC `PullAndRunImage`, and return a typed result.

Request Schema (v1)
- Required fields:
  - `image_name` (string): Docker image reference (example: `nginx:latest`).
  - `resource_requirements` (object): scheduler minimums.
- Optional fields:
  - `command` (string): shell-style args after image (example: `sleep infinity`).
  - `restart_policy` (string): `no | on-failure | always | unless-stopped | on-failure:N`.
  - `machine_type` (string): preference filter (example: `cpu`, `gpu`).
  - `request_id` (string): client-supplied correlation ID; server generates one if absent.

`resource_requirements` shape (v1)
- Required:
  - `memory_gb` (number, > 0)
- Optional:
  - `cpu_cores` (number, > 0)

Success Response Schema
- `status`: `DEPLOYED`
- `request_id`: string
- `selected_machine`:
  - `machine_id`: string
  - `machine_type`: string
  - `grpc_target`: string
- `container`:
  - `container_id`: string
  - `workload_state`: string (expected `RUNNING`)
- `metrics`:
  - `cpu_used`: number
  - `memory_gb_used`: number
- `message`: string

Typed Error Response Schema
- Common fields:
  - `status`: `FAILED`
  - `request_id`: string
  - `error_code`: string
  - `message`: string
  - `retryable`: boolean
  - `details`: object (shape depends on error)

Error Codes (v1)
- `INVALID_INPUT`
  - When payload is malformed or fails validation.
  - `retryable`: `false`.
- `NO_CAPACITY`
  - When no machine can satisfy `resource_requirements` after filters.
  - `retryable`: `true`.
  - `details` includes:
    - `requested`: original resource requirements
    - `considered_machines`: `{ scanned, eligible }`
    - `reject_reasons`: aggregate counts (e.g., `stale_telemetry`, `insufficient_memory`)
    - `retry_hint`: short string
- `MACHINE_CONFIG_ERROR`
  - Candidate machine lacks valid `dra_grpc_target`.
  - `retryable`: `false` until fixed.
- `GRPC_UNAVAILABLE`
  - gRPC target unreachable / deadline exceeded.
  - `retryable`: `true`.
  - `details` includes `grpc_target`, RPC code.
- `REMOTE_EXECUTION_FAILED`
  - RPC returns server/docker failure.
  - `retryable`: context dependent (default `false` unless transient).

HTTP Status Mapping (recommended)
- `200 OK`: deployment succeeded (`DEPLOYED`).
- `400 Bad Request`: `INVALID_INPUT`.
- `409 Conflict`: `NO_CAPACITY`.
- `422 Unprocessable Entity`: `MACHINE_CONFIG_ERROR`.
- `503 Service Unavailable`: `GRPC_UNAVAILABLE`.
- `502 Bad Gateway`: `REMOTE_EXECUTION_FAILED` (upstream execution failure).

Job Persistence Contract (v1)
- Always create/update a job record with:
  - `image_name`, `resource_requirements`, `status`, `created_at`, `updated_at`.
- Status lifecycle:
  - `PENDING` -> `SCHEDULING` -> `DEPLOYED` OR `FAILED_NO_CAPACITY` OR `FAILED_GRPC` OR `FAILED_EXECUTION` OR `FAILED_VALIDATION`.
- Store error metadata for failed terminal states (compact JSON payload).

Minimal Example Payloads
- Request:
  - `{ "image_name": "nginx:latest", "resource_requirements": { "memory_gb": 2 }, "restart_policy": "unless-stopped" }`
- Success:
  - `{ "status": "DEPLOYED", "request_id": "req-123", "selected_machine": { "machine_id": "node-2", "machine_type": "cpu", "grpc_target": "10.0.0.2:50051" }, "container": { "container_id": "abc123", "workload_state": "RUNNING" }, "metrics": { "cpu_used": 13.4, "memory_gb_used": 9.8 }, "message": "Image pulled and container started" }`
- No capacity:
  - `{ "status": "FAILED", "request_id": "req-123", "error_code": "NO_CAPACITY", "message": "No machine currently satisfies requested resources", "retryable": true, "details": { "requested": { "memory_gb": 64 }, "considered_machines": { "scanned": 5, "eligible": 0 }, "reject_reasons": { "insufficient_memory": 4, "stale_telemetry": 1 }, "retry_hint": "Retry in 1-2 minutes or lower memory_gb" } }`
