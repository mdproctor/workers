# casehub-workers -- Consumer Guide

> Worker dispatch infrastructure for the CaseHub ecosystem -- add worker modules as classpath dependencies to gain dispatch capabilities.

**GitHub:** [casehubio/workers](https://github.com/casehubio/workers)
**Tier:** Integration (alongside `claudony` and `casehub-openclaw` in the build order)

---

## Purpose

Each module implements a specific dispatch mechanism (HTTP, Camel, MCP, GitHub Actions, Script, Kubernetes) using the shared `WorkerRuntime` SPI defined in `workers-common`. Application-tier repos add worker modules as classpath dependencies -- the `WorkerLifecycleOrchestrator` auto-discovers and initializes all `WorkerRuntime` beans at startup.

This repo provides the *how* of worker execution (transport, session management, fault handling, retry). The *what* of workers (identity, function, capability vocabulary) lives in `casehub-worker`. The *when* (dispatch scheduling, case orchestration) lives in `casehub-engine`.

## Module Structure

| Module | artifactId | Purpose |
|--------|-----------|---------|
| `workers-common` | `casehub-workers-common` | Shared infrastructure: `WorkerRuntime` SPI, lifecycle orchestrator, async completion registry, fault handling, retry, callback endpoint |
| `workers-http` | `casehub-workers-http` | HTTP dispatch: sync/async via Vert.x WebClient, 3-tier endpoint resolution, URI template interpolation |
| `workers-camel` | `casehub-workers-camel` | Apache Camel dispatch: custom `casehub:` URI scheme, 3-tier capability resolution (SPI/Config/Convention), 300+ connectors |
| `workers-github-actions` | `casehub-workers-github-actions` | GitHub Actions dispatch: `workflow_dispatch` + `repository_dispatch`, fire-and-forget completion |
| `workers-mcp` | `casehub-workers-mcp` | MCP dispatch: Streamable HTTP transport, `tools/list` discovery, session management, dual response parsing (JSON + SSE) |
| `workers-script` | `casehub-workers-script` | Script dispatch: local subprocess execution (shell, Python, JS) via `ProcessBuilder`, stdin JSON delivery |
| `workers-k8s` | `casehub-workers-k8s` | Kubernetes Job dispatch: fabric8 client, watch-based completion via `SharedIndexInformer`, restart recovery from Job labels |
| `workers-testing` | `casehub-workers-testing` | Test fixtures -- **test scope only, never compile/runtime** |

**Activation:** Each worker module activates by classpath presence (`@ApplicationScoped`, no config required to enable). All modules can co-deploy on the same classpath -- `CompositeWorkerExecutionManager` (engine-runtime) discovers all `@WorkerBackend`-qualified execution managers and routes via `supports()`.

## Key Consumer APIs

### WorkerRuntime SPI

Lifecycle contract for a worker runtime -- the infrastructure that executes dispatched work for a specific worker type. Not a task instance, but an executor.

```java
public interface WorkerRuntime {
    String workerType();              // e.g. "mcp", "http", "camel", "github-actions", "script", "k8s"
    WorkerRuntimeStatus status();     // PENDING, RUNNING, FAULTED, STOPPED
    Uni<Void> initialize();           // PENDING -> RUNNING or FAULTED
    Uni<Void> shutdown();             // -> STOPPED
    Set<String> capabilities();       // valid after initialize()
}
```

Implementations must be `@ApplicationScoped`. The `WorkerLifecycleOrchestrator` discovers all beans via CDI `Instance<WorkerRuntime>` and calls `initialize()` at startup (`@Priority(APPLICATION + 10)`). Post-initialization failures are handled by the per-dispatch fault pipeline -- they do not change runtime status.

`WorkerRuntimeStatus` states: `PENDING` (initial), `RUNNING` (accepting dispatches), `FAULTED` (init failed), `STOPPED` (terminal). Recovery: calling `initialize()` on a `FAULTED` runtime retries initialization without requiring application restart.

### Engine Integration -- WorkerExecutionManager SPI

Workers implement the engine's `WorkerExecutionManager` SPI for actual dispatch:

```java
// From casehub-engine-common
public interface WorkerExecutionManager {
    void submit(Long eventLogId, CaseInstance instance, Worker worker,
                Capability capability, Map<String, Object> inputData);
    void submit(Long eventLogId, CaseInstance instance, Worker worker,
                Capability capability, Map<String, Object> inputData,
                String bindingName);
    boolean supports(String capabilityName, String tenancyId);
    int getActiveWorkCount(String workerId);
}
```

All `WorkerExecutionManager` implementations are `@ApplicationScoped` with `@WorkerBackend @Priority(10)`. The `CompositeWorkerExecutionManager` (engine-runtime) discovers backends via the `@WorkerBackend` CDI qualifier and routes dispatch calls to the first backend where `supports()` returns true.

The `submit()` methods are blocking (`void` return). The 5-arg overload delegates to the 6-arg overload with `bindingName = null`.

### Capability Tags

Each worker type uses a distinct capability tag format:

| Worker | Tag format | Example |
|--------|-----------|---------|
| HTTP | Free-form | `send-email`, `validate-address` |
| Camel | Free-form | `send-email`, `process-order` |
| MCP | `mcp:{server}:{tool}` | `mcp:slack:send-message` |
| GitHub Actions | Fixed constants | `github-actions:workflow-dispatch`, `github-actions:repository-dispatch` |
| Script | `script:{name}` | `script:run-analysis` |
| K8s | `k8s:{name}` | `k8s:data-pipeline` |

### Dispatch Mechanisms

#### HTTP

Config-driven endpoint resolution via `HttpEndpointResolver` with three tiers:

1. **Tier 1 -- SPI beans:** `HttpWorkerRoute` implementations (highest priority)
2. **Tier 2 -- Config properties:** `casehub.workers.http.endpoints.{tag}.*`
3. **Tier 3 -- EndpointRegistry:** Tenancy-aware dynamic resolution via `Path.of("http", capabilityTag)`

Two exchange modes via `ExchangeMode`:
- `SYNC` -- Request/response. Response body deserialized as `Map<String, Object>` output.
- `ASYNC` -- Fire-and-forget with callback. Registers a `PendingCompletion` entry. Sends `casehub-worker-id` and `casehub-callback-token` headers for the external system to call back.

URI template interpolation: `{fieldName}` placeholders in URLs are resolved from `inputData` keys. Missing keys throw `PermanentFaultException`.

Transport: Vert.x `WebClient` (event-loop native, non-blocking).

Configuration:
```properties
casehub.workers.http.default-timeout-seconds=30
casehub.workers.http.endpoints.send-email.url=https://mail.internal/send
casehub.workers.http.endpoints.send-email.method=POST
casehub.workers.http.endpoints.send-email.mode=SYNC
casehub.workers.http.endpoints.send-email.timeout-seconds=30
casehub.workers.http.endpoints.send-email.headers.Authorization=Bearer xxx
```

#### Camel

Apache Camel integration with three-tier capability resolution via `CamelCapabilityResolver`:

1. **Tier 1 -- SPI beans:** `CamelWorkerRoute` implementations (programmatic registration)
2. **Tier 2 -- Config properties:** `casehub.workers.camel.capabilities.{tag}={uri}`
3. **Tier 3 -- Convention:** Routes where `routeId` matches a `direct:{routeId}` endpoint are auto-discovered

`CamelWorkerRoute` SPI:
```java
public interface CamelWorkerRoute {
    String capabilityTag();
    String entryUri();
    ExchangePattern exchangePattern();  // InOut = sync, InOnly = async
}
```

Exchange pattern determines dispatch mode:
- `InOut` -- Synchronous via `ProducerTemplate.request()`. Response body (`Map`) becomes output.
- `InOnly` -- Asynchronous via `ProducerTemplate.send()`. Registers `PendingCompletion` entry.

Custom `casehub:` Camel component (`CasehubComponent`, `CasehubEndpoint`, `CasehubProducer`) enables in-route completion signalling. A Camel route can send to `casehub:complete` to resolve a pending completion from within the route itself. The `casehub-worker-id` header on the exchange identifies the dispatch.

CaseHub headers set on all Camel exchanges: `casehub-idempotency`, `casehub-case-id`, `casehub-tenancy-id`, `casehub-task-type`. Async dispatches also set `casehub-worker-id` and `casehub-callback-token`.

Fault detection: exchange exception or `casehub-work-status: FAULTED` header.

#### MCP

Model Context Protocol over Streamable HTTP. Session management with `Mcp-Session-Id` tracking. Protocol version: `2025-06-18` only -- no backwards compatibility with `2024-11-05` HTTP+SSE transport.

Session lifecycle: `McpSessionManager` handles `initialize` / `notifications/initialized` handshake. Concurrent callers share a single initialization `Uni` via `ConcurrentHashMap.computeIfAbsent()` + `Uni.memoize().indefinitely()`. Critical ordering: `onFailure().invoke(remove)` BEFORE `memoize()` to avoid caching failed sessions.

Capability discovery via `tools/list` JSON-RPC:
- `discovery=auto` (default) -- calls `tools/list` at startup. If `tools` config is also set, config acts as an allowlist: configured tools are always registered; discovered tools not in config are ignored.
- `discovery=manual` -- config-only, no `tools/list` call.

Capability tag format: `mcp:{serverName}:{toolName}`.

Dual response parsing: handles both JSON response bodies and SSE (`text/event-stream`) with multi-event parsing. Extracts JSON-RPC response matching the expected request ID from the SSE stream.

Output extraction priority: `structuredContent` (if present, as `Map`) > `content` (as `List<Map>` wrapper) > empty map.

Configuration:
```properties
casehub.workers.mcp.default-timeout-seconds=30
casehub.workers.mcp.servers.slack.url=https://slack.internal/mcp
casehub.workers.mcp.servers.slack.tools=send-message,list-channels
casehub.workers.mcp.servers.slack.timeout-seconds=30
casehub.workers.mcp.servers.slack.discovery=auto
casehub.workers.mcp.servers.slack.headers.Authorization=Bearer xxx
```

#### GitHub Actions

Triggers workflow runs via GitHub API. Fire-and-forget completion model -- 2xx response = dispatched successfully.

Two capability tags:
- `github-actions:workflow-dispatch` -- Triggers `POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches`. Required inputData: `owner`, `repo`, `workflow_id`, `ref`. Optional: `inputs` (map).
- `github-actions:repository-dispatch` -- Triggers `POST /repos/{owner}/{repo}/dispatches`. Required inputData: `owner`, `repo`, `event_type`. Optional: `client_payload`.

Token resolution via `GitHubActionsTokenResolver`: per-org tokens (`casehub.workers.github-actions.tokens.{org}`) with global fallback (`casehub.workers.github-actions.token`). Missing token throws `PermanentFaultException`.

Configuration:
```properties
casehub.workers.github-actions.token=ghp_global_fallback
casehub.workers.github-actions.tokens.myorg=ghp_org_specific
casehub.workers.github-actions.api-base-url=https://api.github.com
```

#### Script

Local subprocess execution via `ProcessBuilder`. Config-driven capability tags with `script:` prefix.

Input delivery: `inputData` serialized as JSON to stdin. Environment variables provide dispatch context: `CASEHUB_CASE_ID`, `CASEHUB_TENANCY_ID`, `CASEHUB_CAPABILITY`, `CASEHUB_IDEMPOTENCY`.

Output parsing: stdout starting with `{` is parsed as JSON object (structured output map). Otherwise wrapped as `{stdout, stderr, exitCode}`.

Bounded stdout/stderr capture: dedicated `ExecutorService` drains both streams with `maxOutputBytes` cap. Continues reading past the cap to prevent SIGPIPE deadlock.

Configuration:
```properties
casehub.workers.script.default-timeout-seconds=300
casehub.workers.script.max-output-bytes=1048576
casehub.workers.script.scripts.run-analysis.command=/usr/bin/python3
casehub.workers.script.scripts.run-analysis.args=scripts/analyze.py
casehub.workers.script.scripts.run-analysis.working-directory=/opt/app
casehub.workers.script.scripts.run-analysis.timeout-seconds=600
casehub.workers.script.scripts.run-analysis.environment.API_KEY=xxx
```

#### Kubernetes

Job-based dispatch via fabric8 client. Watch-based completion via `SharedIndexInformer<Job>` per namespace.

Two Job construction modes:
- **Image-based:** Builds Job from `image`, `command`, `args`, resource limits. Default mode.
- **Template-based:** Loads a YAML template from classpath, overlays CaseHub-managed fields. Template overlay order: `metadata.name`, `metadata.namespace`, `metadata.labels` (merge, CaseHub wins), `spec.backoffLimit`, `spec.activeDeadlineSeconds`, `spec.ttlSecondsAfterFinished`, `spec.template.spec.restartPolicy` (always `Never`, warns if template had different value), first container env vars (appended).

Job naming: `casehub-{slug}-{8-char-hex}`. Slug derived from definition name, max 40 chars with hash suffix for truncation.

Labels: `app.kubernetes.io/managed-by=casehub`, `casehub.io/dispatch-id`, `casehub.io/capability`, `casehub.io/tenancy-id`, `casehub.io/case-id`, `casehub.io/worker-name`, `casehub.io/event-log-id`, `casehub.io/idempotency`. `bindingName` stored as annotation (`casehub.io/binding-name`) because it may exceed the 63-char label value limit.

Input delivery: serialized JSON in `CASEHUB_INPUT_DATA` env var. Validated against `maxInputBytes` (default 256KB) -- etcd has ~1.5MB object size limit.

Output capture: Pod logs from the last Pod (by creation timestamp). JSON object stdout becomes structured output; otherwise wrapped as `{stdout, exitCode}`. Capped by `maxOutputBytes` (default 1MB).

Restart recovery: `processTerminal()` reconstructs `PendingCompletion` from Job labels when the in-memory registry is empty (after application restart). `schedulePersistedEvent()` queries K8s for existing Jobs by label selector before re-dispatching.

Invariants: `restartPolicy: Never` always enforced. `backoffLimit` defaults to 0 -- CaseHub's own fault pipeline owns retry logic, not K8s.

Cleanup: eager delete if `cleanup != RETAIN`, plus `ttlSecondsAfterFinished` safety net (default 600s, minimum 300s).

Configuration:
```properties
casehub.workers.k8s.namespace=default
casehub.workers.k8s.timeout-seconds=3600
casehub.workers.k8s.ttl-after-finished=600
casehub.workers.k8s.backoff-limit=0
casehub.workers.k8s.cleanup=delete
casehub.workers.k8s.max-input-bytes=262144
casehub.workers.k8s.max-output-bytes=1048576
casehub.workers.k8s.jobs.data-pipeline.image=myregistry/pipeline:latest
casehub.workers.k8s.jobs.data-pipeline.namespace=jobs
casehub.workers.k8s.jobs.data-pipeline.command=/bin/sh
casehub.workers.k8s.jobs.data-pipeline.args=-c,run.sh
casehub.workers.k8s.jobs.data-pipeline.cpu-request=100m
casehub.workers.k8s.jobs.data-pipeline.memory-limit=512Mi
casehub.workers.k8s.jobs.data-pipeline.timeout-seconds=1800
casehub.workers.k8s.jobs.data-pipeline.service-account=pipeline-sa
casehub.workers.k8s.jobs.data-pipeline.environment.DB_URL=jdbc:...
casehub.workers.k8s.jobs.data-pipeline.labels.team=platform
```

### Fault Handling

All worker modules share a centralized fault pipeline in `workers-common`:

- `WorkerFaultPublisher` -- publishes `WorkerFaultEvent` onto module-specific Vert.x event bus addresses
- `{Type}WorkerFaultEventHandler` -- `@ConsumeEvent(blocking = true)` stub that delegates to `WorkerFaultHandler`
- `WorkerFaultHandler` -- shared retry body: persist failure log -> `PermanentFaultException` check -> count failures -> `RetryAfterException` check -> compute backoff -> retry or exhaust
- `WorkerRetrySupport` -- shared building blocks: `persistFailureLog`, `countFailedAttempts`, `publishRetriesExhausted`, `resolveRetryPolicy`, `computeBackoffDelayMs`, `parseRetryAfter`

Fault exceptions:
- `PermanentFaultException` -- non-retryable. Skips retry, publishes retries-exhausted immediately.
- `RetryAfterException` -- retryable with explicit delay. Overrides computed backoff.

Default retry policy (when worker has no `ExecutionPolicy`): 3 attempts, 10s FIXED backoff.

Backoff strategies: `FIXED` (constant delay), `EXPONENTIAL` (baseDelay * 2^(attempt-1), capped at 30s), `EXPONENTIAL_WITH_JITTER` (random in [0, exponential cap], capped at 30s).

Fault classification by worker type:

| Condition | HTTP | MCP | GitHub Actions | Script | K8s |
|-----------|------|-----|----------------|--------|-----|
| 4xx (except 429) | Permanent | Permanent | Permanent | -- | -- |
| 429 with Retry-After | RetryAfter | RetryAfter | RetryAfter | -- | -- |
| 422 workflow-dispatch | -- | -- | RetryAfter(60s) | -- | -- |
| 422 repository-dispatch | -- | -- | Permanent | -- | -- |
| 404 with session | -- | Retryable (session expired) | -- | -- | -- |
| 404 without session | -- | Permanent | -- | -- | -- |
| `isError: true` | -- | Retryable | -- | -- | -- |
| JSON-RPC -32600/-32601/-32602/-32700 | -- | Permanent | -- | -- | -- |
| Timeout | Retryable | Retryable | -- | Permanent | -- |
| Non-zero exit | -- | -- | -- | Retryable | -- |
| Command not found | -- | -- | -- | Permanent | -- |
| OOMKilled | -- | -- | -- | -- | Permanent |
| ImagePullBackOff | -- | -- | -- | -- | Permanent |
| BackoffLimitExceeded | -- | -- | -- | -- | Permanent |
| DeadlineExceeded | -- | -- | -- | -- | Permanent |
| Eviction/Preemption | -- | -- | -- | -- | Retryable |
| API 403/404/422 | -- | -- | -- | -- | Permanent |
| API 409 conflict | -- | -- | -- | -- | Retryable |

### Async Completion

`AsyncWorkerCompletionRegistry` tracks pending asynchronous dispatches with TTL-based expiry. Used by HTTP (async mode), Camel (InOnly pattern), and K8s.

- Registration returns a `PendingCompletion` record with generated `dispatchId` and `callbackToken`
- `expireStale()` runs on a schedule (`casehub.workers.async.expiry-check-interval`, default 5m) and fires `CompletionExpiredEvent` for expired entries
- `WorkerCompletionExpiryObserver` converts expired events into faults via `WorkerFaultPublisher`
- Async timeout configurable: `casehub.workers.async.timeout-minutes` (default 60)

`WorkerCallbackResource` receives completion callbacks at `POST /workers/complete/{dispatchId}`:
- Validates `X-Casehub-Callback-Token` header via constant-time comparison (`MessageDigest.isEqual`)
- Token mismatch: re-registers pending entry with remaining TTL, returns 401
- Success with `faulted=false`: publishes completion via `WorkflowCompletionPublisher`
- Success with `faulted=true`: fires `FaultCallbackEvent` -> `WorkerFaultCallbackObserver` -> fault pipeline

## Dependencies

| Repo | What it provides |
|------|-----------------|
| `casehub-platform-api` | `EndpointRegistry`, `EndpointDescriptor`, `EndpointProtocol`, `RetryPolicy`, `BackoffStrategy`, `ExecutionPolicy` |
| `casehub-worker-api` | `Worker`, `Capability`, `WorkerFunction`, `WorkerResult`, `WorkResult` |
| `casehub-engine-api` | `WorkerStatusListener`, `CaseHubEventType`, `EventStreamType` |
| `casehub-engine-common` | `WorkerExecutionManager`, `WorkerBackend`, `WorkflowExecutionCompleted`, `WorkerRetriesExhaustedEvent`, `CaseInstance`, `EventLog`, `EventLogRepository`, `CaseInstanceRepository`, `EventBusAddresses` |
| Quarkus Camel BOM | Camel module only -- `camel-quarkus-core`, `camel-quarkus-direct` |
| fabric8 kubernetes-client | K8s module only -- via Quarkus Kubernetes Client extension |

## What It Does NOT Do

- Define worker identity, function, or capability vocabulary -- that is `casehub-worker`
- Schedule or orchestrate work -- that is `casehub-engine`
- Provide `WorkerFunction` implementations -- those live in consuming repos (e.g. `AgentWorkerFunction` in engine)
- Manage worker state machines or task instances -- `WorkerRuntime` is an executor lifecycle, not a task instance lifecycle
- Provide a UI or management API for worker configuration -- workers are configured via Quarkus config properties
- Run as a standalone application -- these are library modules consumed by application-tier deployments
