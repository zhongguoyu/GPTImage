# High-Performance Account Scheduler Design

**Status:** Review requested

**Base branch:** `feat/sqlite-log-system`

## Goal

Replace the current account round-robin and thread-per-image-task behavior with a bounded, single-process scheduler that keeps text, image generation, and image editing isolated while making account selection efficient for any account count.

The scheduler must improve throughput and overload behavior without introducing Redis or another external service. SQLite is used for configuration snapshots, health snapshots, and recoverable task metadata, not for hot-path account selection or queue operations.

## Confirmed Constraints

- Deployment target is one Uvicorn worker per scheduler instance.
- No Redis, message broker, or other external coordination service is required.
- The scheduler is an in-memory runtime component.
- SQLite is the persistence layer for scheduler snapshots and recoverable task metadata.
- Text, image generation, and image editing use separate resource lanes.
- Each lane has a bounded queue and fixed worker capacity.
- Capacity exhaustion may return `429` or `503` quickly instead of waiting indefinitely.
- Account selection must not scan every account on every request.
- Account count is variable; 20, 200, and 2000 accounts use the same algorithm.
- Image request cost is based on `n`; an `n=4` request consumes four work units.
- Existing image generation and image edit behavior remains available through the current API routes.
- This design covers `/v1/images/generations`, `/v1/images/edits`, image-capable chat or response requests, and the asynchronous image task API.

## Current Problems

The current text path uses a process-local round-robin index without per-account text concurrency or quota isolation. The image path uses a process-local round-robin index with a fixed per-account image slot count, but it can wait indefinitely when slots are full. The asynchronous image task service starts one daemon thread per submitted task instead of using a bounded executor or queue.

The result is uneven account load, incomplete failure isolation, unbounded waiting under bursts, and possible thread or connection accumulation before the service eventually returns gateway errors.

The current image implementation also retries some polling failures by selecting another account. If an upstream image task was already accepted, blindly submitting again can create duplicate generations.

## Architecture

```text
HTTP request
    |
    v
Request classifier and cost calculator
    |
    +--> text lane
    +--> image-generation lane
    +--> image-edit lane
             |
             v
      bounded admission queue
             |
             v
      fixed lane workers
             |
             v
      AccountLeaseManager
             |
             v
      capability indexes and ready heaps
             |
             v
      upstream request
```

The scheduler consists of these focused units:

### Scheduler facade

Classifies a request, calculates its work-unit cost, applies the lane deadline, and returns either a running request handle, a queued handle, or an overload result.

### Lane scheduler

Owns one bounded queue, one fixed worker pool, admission counters, queue aging, and lane-level metrics. Text, image generation, and image editing each have an independent lane.

### Account lease manager

Owns account runtime state, capability indexes, per-account permits, global lane permits, lease IDs, health transitions, and release/recovery logic. All lease changes are atomic under one in-process scheduler lock.

### Snapshot writer

Receives coalesced state changes and writes them asynchronously to `data/scheduler.db`. It never runs synchronously inside the account-selection hot path.

### Task recovery service

Persists asynchronous image task state and resumes only tasks whose remote task identity is known. It does not recreate an unknown submission after a restart without an idempotency mechanism.

## Resource Lanes and Capacity

Each lane has independent settings:

```text
global_concurrency
queue_max_items
queue_max_cost
queue_wait_timeout
request_deadline
worker_count
```

Work-unit costs are:

- text request: `1`;
- image generation: `n`;
- image edit: `n`, plus a separate input-size limit;
- streaming text: `1` for the lifetime of the upstream stream.

The scheduler applies capacity in layers:

```text
system total capacity
    -> lane global capacity
        -> account lane capacity
            -> account total capacity
```

An account that supports both image generation and image editing may be present in both lane indexes, but its total image capacity remains bounded. Separate lane limits prevent long image polling from consuming all text capacity.

Global capacity is configured from machine resources, upstream limits, and measured p95 latency. It is not derived as `account_count * per_account_limit`. Adding accounts increases availability and failover options, but does not automatically increase upstream concurrency.

Idle capacity may be borrowed between lanes only through an explicit configured ceiling. Text keeps a reserved minimum so that image traffic cannot starve interactive text requests.

## Account State and Capability Indexes

Runtime state for each account contains:

```text
account_id
capabilities
status
quota_snapshot
text_inflight
image_generation_inflight
image_edit_inflight
image_total_inflight
lane_limits
ewma_latency_ms
recent_successes
recent_failures
consecutive_failures
breaker_state
cooldown_until
token
token_generation
lease_generation
```

Accounts are indexed by the exact routing dimensions needed by a request:

```text
(text, model)
(image_generation, model, plan_type, source_type)
(image_edit, model, plan_type, source_type)
```

Each index has:

- a ready min-heap ordered by normalized load score;
- a cooldown min-heap ordered by `cooldown_until`;
- a revision number for lazy removal of stale heap entries.

Adding or removing an account updates only the relevant capability indexes. On each selection, stale heap entries are discarded from the top; the scheduler does not rebuild or scan the full account set.

## Account Selection

The selection score is:

```text
score =
    0.45 * lane_load_ratio
  + 0.25 * total_load_ratio
  + 0.20 * normalized_ewma_latency
  + 0.10 * recent_failure_penalty
  + tiny_random_jitter
```

The coefficients are configuration defaults, not account-count-dependent behavior. They should be tuned from production metrics.

Selection flow:

1. A background cooldown timer promotes accounts whose cooldown deadline has passed.
2. Pop a bounded number of stale or ineligible heap entries.
3. Select the lowest-score ready account.
4. Under the scheduler lock, verify global lane capacity and account permits.
5. Create a lease with a unique ID and deadline.
6. Increment all relevant in-flight counters and publish the updated heap key.

Lease acquisition and release are both constant-time counter operations plus heap updates, giving approximately `O(log N)` selection for `N` candidate accounts. Startup index construction is `O(N log N)` and is acceptable because it happens once per process.

The scheduler uses no per-account worker threads. Worker count depends on the lane and host capacity, not on the number of accounts.

## Leases and Cleanup

Every upstream execution receives a lease containing:

```text
lease_id
account_id
lane
cost
created_at
deadline
token_generation
```

All execution paths release the lease in `finally` blocks. Long-running image polling sends bounded heartbeats. A watchdog reclaims leases that pass their deadline without a heartbeat or completion. Client disconnects and request cancellation release the lease immediately.

Token refresh is single-flight per account. A request that sees an old token generation waits for the one refresh operation or receives the refresh result; it does not start a second concurrent refresh for that account.

## Admission, Queues, and Backpressure

The admission path is:

```text
validate request and cost
    -> check lane capacity
    -> check queue item and cost limits
    -> check candidate account availability
    -> run immediately, queue, or reject
```

Queue entries are lightweight request handles or futures. Waiting requests do not create one background thread each.

When capacity is available, the scheduler takes the fast path directly to a fixed worker. When capacity is temporarily unavailable and the queue has room, the request enters the lane queue. When the queue is full, the request is rejected immediately.

Response behavior:

- `429` means the lane or queue is at capacity; include `Retry-After`.
- `503` means no account currently satisfies the requested capability or all candidates are in a provider outage state.
- Queue timeout returns the same overload class rather than keeping the connection open indefinitely.

Within each lane, requests use priority plus aging. Interactive text can be favored over long-running image work, but waiting requests gain priority over time so that a low-priority request cannot starve forever. A retry consumes the same request deadline and cannot jump ahead indefinitely.

## Worker Execution and Streaming

Each lane uses a dedicated fixed executor or equivalent fixed worker mechanism. The scheduler does not rely on the framework's default synchronous thread pool for long image polling operations.

For streaming text, the worker holds one text lease while it consumes the upstream stream and forwards bounded chunks through a per-request channel. If the client disconnects or the channel cannot be consumed before the request deadline, the upstream stream is cancelled and the lease is released.

For image generation and editing, each output unit gets its own work item. A request with `n=4` therefore occupies four image work units and completes only after its requested output units have reached terminal states or the request deadline expires.

The asynchronous image task API submits to the same image lanes. It does not create a daemon thread per task. Durable task metadata is created before admission, while actual execution is performed by fixed workers.

## Retry and Error Policy

Retries use the error class, output state, and remaining request budget.

### Text

- `401` or invalid token: refresh the selected account once, then try another account if no content was emitted.
- `429`: cool down that account and make a bounded alternate-account attempt if no content was emitted.
- Network error, timeout, or `5xx`: record health impact and make a bounded alternate-account attempt before the deadline.
- Error after partial output: terminate the stream; do not replay it through another account.
- Parameter, model, and content-policy errors: fail the request without penalizing the account.

### Image generation and editing

- Failure before remote task acceptance: retry another account within the request budget.
- Known remote task ID: continue polling that task; do not submit a duplicate.
- Unknown submission result: use an idempotency key or mark the task as unknown; do not blindly submit again.
- Polling timeout: update latency and health metrics but do not automatically mark the account invalid.
- Input, parameter, and content-policy errors: fail without account retry.

Every request has `request_deadline`, `max_account_attempts`, `max_retry_cost`, and an attempted-account set. Retries release the old lease before acquiring a new one and cannot bypass lane or account capacity.

## Circuit Breakers

Circuit state is maintained per account and capability:

```text
closed -> open -> half-open -> closed
                         \-> open
```

- `closed`: normal selection.
- `open`: removed from the ready heap until cooldown expires.
- `half-open`: admits a small number of probe requests.
- successful probes close the breaker; failed probes extend the cooldown.

Authentication failures, rate limits, network failures, timeouts, and server errors use different penalties. A picture-generation outage does not remove the account's text capability. Cooldowns use exponential backoff with random jitter to prevent synchronized recovery bursts.

## SQLite Persistence and Recovery

Scheduler snapshots use a separate `data/scheduler.db` file so health writes cannot contend with the log query workload in `data/logs.db`.

SQLite uses WAL, a configured busy timeout, and short transactions. The snapshot writer coalesces changes by account and flushes periodically or during graceful shutdown. It stores:

```text
account_id
capability
quota_snapshot
breaker_state
cooldown_until
ewma_latency
recent_success_count
recent_failure_count
last_error_class
updated_at
```

The following are not written on every request:

- queue depth;
- in-flight counters;
- active leases;
- every polling heartbeat;
- every retry attempt.

At startup:

1. Load account configuration and the latest snapshots.
2. Rebuild capability indexes and heaps.
3. Discard old process leases and reset in-flight counters.
4. Resume image tasks only when a remote task ID is known.
5. Mark tasks with unknown submission outcome as recoverable-unknown instead of duplicating them.
6. Restore breaker cooldowns and use half-open probes before normal release.

Direct HTTP requests that were in flight during a process restart cannot be resumed. Only the asynchronous task API can provide durable recovery semantics.

## Observability and Logging

Metrics are maintained in memory and periodically aggregated. Required lane metrics include:

```text
active
queue_depth
queue_cost
admitted
rejected
queue_wait_ms
service_latency_ms
p50 / p95 / p99
retry_count
timeout_count
breaker_open_count
```

Required account metrics include:

```text
inflight
success_rate
error_rate
ewma_latency
cooldown_remaining
quota
breaker_state
```

Operational logs record request summaries and state transitions, not every poll heartbeat. A scheduler event includes the lane, outcome, latency, retry count, and breaker transition when applicable. Request bodies, image bytes, and generated output payloads are never written to the scheduler state store.

## Configuration Shape

The implementation should expose lane settings rather than hard-code them to an account count:

```json
{
  "scheduler": {
    "enabled": true,
    "text": {
      "worker_count": 32,
      "global_concurrency": 128,
      "queue_max_items": 256,
      "queue_max_cost": 256,
      "queue_wait_timeout_ms": 1500
    },
    "image_generation": {
      "worker_count": 8,
      "global_concurrency": 32,
      "queue_max_items": 64,
      "queue_max_cost": 128,
      "queue_wait_timeout_ms": 1000
    },
    "image_edit": {
      "worker_count": 4,
      "global_concurrency": 16,
      "queue_max_items": 32,
      "queue_max_cost": 64,
      "queue_wait_timeout_ms": 1000
    }
  }
}
```

The numbers above are starting examples only. Production values must be calibrated against upstream limits, machine resources, and p95 latency. The implementation must validate that queue cost and worker limits are positive and must prevent a lane configuration from exceeding the system total capacity.

## Testing and Acceptance Criteria

Add focused tests for:

- capability indexing and model/plan/source routing;
- heap selection without full account scans;
- lease acquisition, release, expiry, and cancellation;
- no negative or over-limit in-flight counters under concurrent operations;
- text, image generation, and image edit lane isolation;
- `n=4` consuming four work units;
- queue limits and fast `429`/`503` responses;
- priority aging and starvation prevention;
- token-refresh single-flight;
- retry classification and request deadlines;
- per-capability circuit breaker transitions;
- known remote image task recovery and unknown submission handling;
- SQLite snapshot coalescing and restart recovery;
- account pool sizes of 1, 20, 200, and 2000;
- load behavior with no per-request thread creation.

The feature is accepted when:

- hot-path account selection does not scan all accounts or synchronously write SQLite;
- all three lanes have bounded capacity and independent overload behavior;
- no request can leak a lease after cancellation, timeout, or exception;
- image traffic cannot exhaust the reserved text capacity;
- overload returns an explicit `429` or `503` instead of indefinite waiting and eventual `502`;
- known remote image tasks can resume after restart without duplicate submission;
- breaker state is isolated per account capability;
- metrics identify queue saturation, account hotspots, retry storms, and stale leases;
- throughput and p95 latency are validated with representative account counts and upstream response times.

## Out of Scope

- Redis, Kafka, or another external coordination service.
- Multi-worker or multi-replica shared scheduling.
- Replacing the existing account storage backend.
- Distributed global rate limiting across multiple containers.
- PPT/PSD export scheduling, which remains a separate capability until it receives its own lane requirements.
