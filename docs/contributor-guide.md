# casehub-workers -- Contributor Guide

> Internal architecture, module details, and extension points for platform builders working on worker dispatch infrastructure.

**GitHub:** [casehubio/workers](https://github.com/casehubio/workers)

---

## Internal Architecture

### Module Architecture Pattern

Every worker module follows a consistent five-class pattern:

| Class | Role |
|-------|------|
| `{Type}WorkerRuntime` | Implements `WorkerRuntime`. Initializes transport and discovers capabilities. |
| `{Type}CapabilityResolver` / `{Type}ServerResolver` | Implements `WorkerCapabilityResolver<T>`. Maps capability tags to concrete targets (endpoints, servers, definitions). |
| `{Type}WorkerExecutionManager` | Implements `WorkerExecutionManager`. Dispatches work at execution time -- serializes input, sends to target, handles response. |
| `{Type}ReactiveWorkerProvisioner` | Implements `ReactiveWorkerProvisioner`. Capability probe at case planning time -- validates that the worker can handle requested capabilities. |
| `{Type}WorkerFaultEventHandler` | Observes Vert.x EventBus messages on module-specific fault address. Delegates to `WorkerFaultHandler` for retry logic. |

### Fault Pipeline Architecture

Centralized in `workers-common`. Per-module fault event handlers are 5-line stubs:

- `WorkerFaultPublisher` -- parameterized by fault address, publishes `WorkerFaultEvent`
- `WorkerFaultHandler` -- shared retry body: persist -> `PermanentFaultException` check -> count -> `RetryAfterException` check -> retry-or-exhaust. Always uses `emitOn(workerPool)` before re-dispatch
- `WorkerCompletionExpiryObserver` -- generic `@ObservesAsync CompletionExpiredEvent`, routes via `faultAddress` from `PendingCompletion`
- `WorkerFaultCallbackObserver` -- generic `@ObservesAsync FaultCallbackEvent`, routes via `faultAddress` from `PendingCompletion`

Worker faults fire on worker-specific addresses (`CAMEL_WORKER_FAULT`, `HTTP_WORKER_FAULT`, etc.), NOT `WORKFLOW_EXECUTION_FAILED` -- Quartz listens on the latter and would double-process.

### EndpointRegistry Resolution

Three-tier resolution order: Tier 1 (SPI beans) > Tier 2 (config) > Tier 3 (EndpointRegistry). Single registry call with tenancyId -- registry handles tenant -> platform-global fallback internally. `capabilities()` stays static (SPI + config only).

Path conventions: HTTP uses `Path.of("http", capabilityTag)`, MCP uses `Path.of("mcp", serverName)`. Protocol check on descriptors: HTTP resolver accepts `EndpointProtocol.HTTP` only, MCP accepts `EndpointProtocol.MCP` only.

### BindingName Correlation

`bindingName` propagation from casehub-engine through worker dispatch enables tracing which YAML binding triggered a worker execution. `WorkerCorrelationContext` carries `bindingName` alongside `caseId` and `eventLogId`. All 6 worker modules override the 6-arg `submit()` to thread `bindingName` through to transport-specific metadata.

### @WorkerBackend Qualifier

CDI qualifier applied to all `WorkerExecutionManager` implementations. Each execution manager declares `@WorkerBackend @Priority(N)` and implements `supports(String capabilityName, String tenancyId)` to enable CDI-based dynamic dispatch. `CompositeWorkerExecutionManager` (engine-runtime) discovers backends via this qualifier.

## Module Details

### workers-common Key Types

| Type | Purpose |
|------|---------|
| `WorkerRuntime` | Lifecycle SPI -- `initialize()`, `shutdown()`, `capabilities()`, `status()` |
| `WorkerRuntimeStatus` | `PENDING` -> `RUNNING` -> `STOPPED`, `PENDING` -> `FAULTED` -> `STOPPED`, `FAULTED` -> `RUNNING` (recovery) |
| `WorkerLifecycleOrchestrator` | Discovers all `WorkerRuntime` beans, calls `initialize()` at startup (`@Priority(APPLICATION + 10)`), `shutdown()` at `@PreDestroy`. Sequential across types, fail-open per worker |
| `WorkerCapabilityResolver<T>` | Tenancy-aware endpoint resolution SPI -- `resolve(capabilityTag, tenancyId)`, `firstMatch(capabilities, tenancyId)`, `capabilities()` |
| `PendingCompletion` | Registry entry per async dispatch -- carries `dispatchId`, `workerType`, `faultAddress`, `callbackToken`, `capability`, `eventLogId` |
| `WorkerCorrelationContext` | Per-dispatch context -- `CaseInstance`, `Worker`, `idempotency`, `tenancyId`, `bindingName` |
| `AsyncWorkerCompletionRegistry` | In-memory pending completion store; `expireStale()` fires `CompletionExpiredEvent` CDI async |
| `WorkflowCompletionPublisher` | Fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` via `eventBus.publish()` |
| `WorkerCallbackResource` | `POST /workers/complete/{dispatchId}` -- REST callback for external systems |
| `WorkerRetrySupport` | Shared retry building blocks -- `persistFailureLog`, `countFailedAttempts`, `publishRetriesExhausted`, `resolveRetryPolicy`, `computeBackoffDelayMs`, `parseRetryAfter` |
| `PermanentFaultException` | Worker-agnostic "don't retry" signal |
| `RetryAfterException` | Worker-agnostic "retry after delay" signal |
| `WorkerFaultEvent` | Local fault event record -- `caseInstance`, `worker`, `capability`, `inputDataHash`, `eventLogId`, `cause`, `bindingName` |
| `WorkerFaultPublisher` | Generic fault publisher -- parameterized by fault address |
| `WorkerFaultHandler` | Shared fault handler body -- persist -> check -> count -> retry-or-exhaust |
| `CasehubWorkerHeaders` | Header name constants shared across all worker types |

### workers-http

`HttpEndpointResolver` provides 3-tier capability tag -> `ResolvedEndpoint` resolution. `HttpWorkerExecutionManager` dispatches via Vert.x WebClient -- reactive-native. Two exchange modes: `SYNC` (request/response) and `ASYNC` (fire-and-forget with callback). `HttpWorkerRoute` SPI interface for Tier 1 endpoint registration.

Key rules:
- HTTP 4xx (except 429) throws `PermanentFaultException` -- skips retry
- HTTP 429 with `Retry-After` header throws `RetryAfterException` -- overrides configured backoff

### workers-camel

Custom `casehub:` Camel component (`CasehubComponent`, `CasehubEndpoint`, `CasehubProducer`) bridges Camel routes to CaseHub worker infrastructure. `CamelCapabilityResolver` maps capability tags to Camel route URIs.

### workers-mcp

Streamable HTTP transport with JSON-RPC. `McpSessionManager` handles session lifecycle with eager init at startup. `McpServerResolver` supports config + discovery + EndpointRegistry resolution.

Key rules:
- Protocol version: `2025-06-18` only. No backwards compatibility with `2024-11-05` HTTP+SSE transport
- `discovery=auto` (default) calls `tools/list` at startup; `discovery=manual` is config-only
- `tools` config property is an allowlist when `discovery=auto` -- config tools always registered; discovered tools not in config are ignored
- `isError: true` is retryable (not permanent) -- MCP spec example is "API rate limit exceeded"
- 404 with active `Mcp-Session-Id` -> session expired, retryable. 404 without session -> `PermanentFaultException`
- Session initialization uses `ConcurrentHashMap.computeIfAbsent` + `Uni.memoize().indefinitely()` -- `onFailure().invoke(remove)` BEFORE `memoize()`

### workers-github-actions

Dispatches via GitHub API (`workflow_dispatch` + `repository_dispatch`). `GitHubActionsTokenResolver` manages per-org + global PAT resolution.

Key rules:
- 422 on `workflow-dispatch` throws `RetryAfterException(60_000)` -- workflow_dispatch trigger caching
- 422 on `repository-dispatch` throws `PermanentFaultException` -- malformed request
- `ref` is required for `workflow-dispatch` -- GitHub API rejects requests without it

### workers-script

Local subprocess execution via `ProcessBuilder`. `ScriptDefinitionResolver` maps config-driven capability tags (prefix `script:`) to `ScriptDefinition` records.

Key rules:
- Stdin delivers `inputData` as JSON. Env vars: `CASEHUB_CASE_ID`, `CASEHUB_TENANCY_ID`, `CASEHUB_CAPABILITY`, `CASEHUB_IDEMPOTENCY`
- Stdout parsing: JSON object -> structured output map; else -> raw wrapper `{stdout, stderr, exitCode}`
- Timeout -> `PermanentFaultException` (diverges from HTTP/MCP where timeout is retryable)
- Non-zero exit -> `RuntimeException` (retryable). Command not found -> `PermanentFaultException`
- Bounded stdout/stderr capture with dedicated `ExecutorService` for stream draining

### workers-k8s

Kubernetes Job dispatch via fabric8 client. Watch-based completion via `SharedIndexInformer<Job>` per namespace.

Key rules:
- `restartPolicy: Never` always enforced; `backoffLimit` defaults to 0 -- CaseHub's fault pipeline owns retry logic
- Job naming: `casehub-{slug}-{8-char-hex}` (max 57 chars)
- Labels: `app.kubernetes.io/managed-by=casehub`, `casehub.io/dispatch-id`, `casehub.io/capability`, `casehub.io/tenancy-id`
- Recovery metadata: `casehub.io/case-id`, `casehub.io/worker-name`, `casehub.io/event-log-id`, `casehub.io/idempotency` labels; `casehub.io/binding-name` as annotation (may exceed 63-char label limit)
- Restart recovery: `processTerminal()` reconstructs `PendingCompletion` from Job labels when registry is empty
- Eager resolver initialization via `@PostConstruct` eliminates race between engine recovery and worker initialization
- Cleanup: eager delete if `cleanup != RETAIN`, plus `ttlSecondsAfterFinished` safety net (default 600s, minimum 300s)
- Fault classification: `BackoffLimitExceeded`, `DeadlineExceeded`, `OOMKilled`, `ImagePullBackOff` -> permanent; eviction/preemption -> retryable; API 403/404/422 -> permanent; 409 conflict -> retryable
- Input size validated against `maxInputBytes` (default 256KB); etcd has ~1.5MB object size limit
- Template overlay order: `metadata.name`, `metadata.namespace`, `metadata.labels` (merge, CaseHub wins), `spec.backoffLimit`, `spec.activeDeadlineSeconds`, `spec.ttlSecondsAfterFinished`, `spec.template.spec.restartPolicy`, first container env vars (append CaseHub env vars)

### workers-testing

`WorkerTestSupport` -- factory methods for test `CaseInstance`, `Worker`, `Capability` instances. **Test scope only, never compile/runtime.**

## Key Invariants

- Workers are stateless -- all state in the case instance or external system, never in provisioner beans
- `tenancyId` propagated through all calls -- bind in Repository layer only
- Completion fires `eventBus.publish()` on `WORKER_EXECUTION_FINISHED` -- never `request()`. Two consumers exist; `publish()` delivers to both
- Retry logic: `failureCount < retryPolicy.maxAttempts()` (strict `<`); null policy defaults to `new RetryPolicy()` (3 attempts, 10s FIXED)
- `WorkerFaultHandler` always uses `emitOn(Infrastructure.getDefaultWorkerPool())` before re-dispatch -- correct for all workers regardless of blocking/reactive
- Worker runtime status reflects initialization outcome only -- post-init dispatch failures go through the per-dispatch fault pipeline
- FAULTED -> RUNNING recovery: calling `initialize()` on a FAULTED runtime retries initialization
- Build order: `workers-common` must be first in parent POM `<modules>` -- all others depend on it

## Depended On By

| Repo | What it uses |
|------|-------------|
| Application-tier repos that need worker dispatch | Add specific worker modules as classpath dependencies; `WorkerLifecycleOrchestrator` auto-discovers them |

## Cross-Repo Dependencies

| Dependency | Why |
|---|---|
| `casehub-worker-api` | `Worker`, `Capability`, `WorkerFunction`, `WorkerResult`, `WorkerOutcome` |
| `casehub-engine-api` | `ReactiveWorkerProvisioner`, `ProvisionContext`, `ProvisionResult`, `WorkResult`, `CaseHubEventType` |
| `casehub-engine-common` | `WorkerExecutionManager`, `WorkerBackend`, `WorkflowExecutionCompleted`, `CaseInstance`, `EventLog`, `EventLogRepository` |
| `casehub-platform-api` | `EndpointRegistry`, `EndpointDescriptor`, `EndpointProtocol`, `RetryPolicy`, `BackoffStrategy` |

Open cross-repo dependency: engine#676 (add `bindingName` parameter to `WorkerExecutionManager.submit()`).

## Current State

- All 8 modules (common, http, camel, github-actions, mcp, script, k8s, testing) on main with tests
- Consistent five-class pattern across all dispatch modules
- `@WorkerBackend` qualifier enables CDI-based dynamic dispatch
- MCP module supports Streamable HTTP transport with configurable `tools/list` discovery (`discovery=auto|manual`)
- Camel module includes a custom `casehub:` Camel component for bidirectional integration
- K8s module supports restart recovery via enriched Job labels and eager resolver initialization
- `AsyncWorkerCompletionRegistry` with TTL-based expiry for async dispatch patterns
- `WorkerFaultHandler` with configurable retry, backoff, `RetryAfterException` support, and permanent fault detection

## Design Documents

- `docs/superpowers/specs/2026-06-08-casehub-workers-camel-design.md` -- fully approved Camel worker design (7 review cycles)
- `ARC42STORIES.MD` -- primary architecture record; check sections 9-10 after worker module, SPI, or fault pipeline changes
