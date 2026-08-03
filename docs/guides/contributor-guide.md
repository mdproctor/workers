# casehub-workers -- Contributor Guide

> Internal architecture, module details, and extension points for platform builders working on worker dispatch infrastructure.

**GitHub:** [casehubio/workers](https://github.com/casehubio/workers)

---

## Internal Architecture

### Module Architecture Pattern

Every worker module follows a consistent four-class pattern:

| Class | Role |
|-------|------|
| `{Type}WorkerRuntime` | Implements `WorkerRuntime`. Initializes transport, resolver, and discovers capabilities. |
| `{Type}CapabilityResolver` / `{Type}ServerResolver` / `{Type}DefinitionResolver` | Implements `WorkerCapabilityResolver<T>`. Maps capability tags to concrete targets (endpoints, servers, definitions). |
| `{Type}WorkerExecutionManager` | Implements `WorkerExecutionManager` with `@WorkerBackend @Priority(10)`. Dispatches work -- serializes input, sends to target, handles response, publishes completion or fault. |
| `{Type}WorkerFaultEventHandler` | `@ConsumeEvent(blocking = true)` on module-specific fault address. 5-line stub that delegates to `WorkerFaultHandler`. |

The K8s module adds additional types beyond this pattern: `K8sJobBuilder` (Job construction), `K8sJobInformerManager` (watch-based completion, restart recovery), `K8sJobOutputCapture` (Pod log capture), `JobDefinition` (config record), `CleanupPolicy` (enum).

### Fault Pipeline Architecture

Centralized in `workers-common`. Per-module fault event handlers are 5-line stubs that all delegate to the same `WorkerFaultHandler`:

```java
@ApplicationScoped
public class HttpWorkerFaultEventHandler {
    @Inject WorkerFaultHandler workerFaultHandler;

    @ConsumeEvent(value = HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT, blocking = true)
    public void onFault(WorkerFaultEvent event) {
        workerFaultHandler.handleFault(event);
    }
}
```

The pipeline consists of:

- `WorkerFaultPublisher` -- two `fault()` overloads: one from explicit parameters, one from `PendingCompletion`. Both publish `WorkerFaultEvent` onto the Vert.x event bus at the specified `faultAddress`.
- `WorkerFaultHandler` -- shared retry body: `persistFailureLog` -> `PermanentFaultException` check -> `countFailedAttempts` -> `RetryAfterException` check -> `computeBackoffDelayMs` -> `reloadAndResubmit` or `publishRetriesExhausted`. Retry re-dispatch reloads the `EventLog` and calls `workerExecutionManager.submit()` with the 6-arg overload (preserving `bindingName`). Delay is applied via `Thread.sleep()` since all fault handlers run on the worker pool (`blocking = true`).
- `WorkerCompletionExpiryObserver` -- `@ObservesAsync CompletionExpiredEvent`. Fired by `AsyncWorkerCompletionRegistry.expireStale()` when a pending completion exceeds its TTL. Routes to fault publisher with `"Async timeout"` message.
- `WorkerFaultCallbackObserver` -- `@ObservesAsync FaultCallbackEvent`. Fired by `WorkerCallbackResource` when an external callback reports `faulted=true`. Routes to fault publisher.

Worker faults fire on worker-specific addresses (`CAMEL_WORKER_FAULT`, `HTTP_WORKER_FAULT`, `MCP_WORKER_FAULT`, `SCRIPT_WORKER_FAULT`, `GITHUB_ACTIONS_WORKER_FAULT`, `K8S_WORKER_FAULT`), NOT `WORKFLOW_EXECUTION_FAILED` -- Quartz listens on the latter and would double-process.

### Completion Path

`WorkflowCompletionPublisher.complete()` fires `WorkflowExecutionCompleted.approved()` on `EventBusAddresses.WORKER_EXECUTION_FINISHED` via `eventBus.publish()`. Uses `publish()`, never `request()` -- two consumers exist; `publish()` delivers to both.

`WorkerStatusPublisher` delegates dispatch-level lifecycle events to the engine's `WorkerStatusListener` SPI: `onWorkerStarted(dispatchId, sessionMeta)`, `onWorkerCompleted(dispatchId, result)`, `onWorkerStalled(dispatchId)`.

### EndpointRegistry Resolution

Three-tier resolution order: Tier 1 (SPI beans) > Tier 2 (config) > Tier 3 (EndpointRegistry). Single registry call with `tenancyId` -- registry handles tenant -> platform-global fallback internally. `capabilities()` stays static (SPI + config only) -- EndpointRegistry provides runtime-resolved capabilities not reported by `capabilities()`.

Path conventions:
- HTTP uses `Path.of("http", capabilityTag)`, accepts `EndpointProtocol.HTTP` only
- MCP uses `Path.of("mcp", serverName)`, accepts `EndpointProtocol.MCP` only

`WorkerCapabilityResolver<T>` interface:
```java
public interface WorkerCapabilityResolver<T> {
    T resolve(String capabilityTag, String tenancyId);
    Optional<String> firstMatch(Set<String> capabilities, String tenancyId);
    Set<String> capabilities();
    default boolean canResolve(String capabilityTag, String tenancyId) {
        return capabilities().contains(capabilityTag);
    }
}
```

HTTP and MCP resolvers override `canResolve()` to also check EndpointRegistry. Camel uses SPI/Config/Convention tiers only (no EndpointRegistry).

### BindingName Correlation

`bindingName` propagation from casehub-engine through worker dispatch enables tracing which YAML binding triggered a worker execution. `WorkerCorrelationContext` carries `bindingName` alongside `caseId`, `idempotency`, and `tenancyId`. All 6 worker modules implement both the 5-arg `submit()` (delegates with `null`) and 6-arg `submit(bindingName)` overload.

`bindingName` flows through:
- `WorkerCorrelationContext` -> dispatch headers/env vars
- `WorkerFaultEvent` -> retry re-dispatch (6-arg `submit()`)
- `WorkflowCompletionPublisher` -> `WorkflowExecutionCompleted.approved()`
- `WorkerRetrySupport.publishRetriesExhausted()` -> `WorkerRetriesExhaustedEvent`
- K8s: stored as Job annotation `casehub.io/binding-name` (not label -- may exceed 63-char limit). Recovered during restart recovery.

### @WorkerBackend Qualifier

CDI qualifier applied to all `WorkerExecutionManager` implementations. Each execution manager declares `@WorkerBackend @Priority(10)` and implements `supports(String capabilityName, String tenancyId)` to enable CDI-based dynamic dispatch. `CompositeWorkerExecutionManager` (engine-runtime) discovers backends via this qualifier.

### Callback Security

`WorkerCallbackResource` at `POST /workers/complete/{dispatchId}` validates the `X-Casehub-Callback-Token` header using constant-time comparison (`MessageDigest.isEqual`). On token mismatch, the pending completion is re-registered with its remaining TTL and the endpoint returns 401. This prevents timing attacks from leaking valid tokens while preserving the completion for a legitimate retry.

## Module Details

### workers-common -- Complete Type Inventory

| Type | Kind | Purpose |
|------|------|---------|
| `WorkerRuntime` | interface (SPI) | Lifecycle contract: `initialize()`, `shutdown()`, `capabilities()`, `status()`, `workerType()` |
| `WorkerRuntimeStatus` | enum | `PENDING` -> `RUNNING` -> `STOPPED`, `PENDING` -> `FAULTED` -> `STOPPED`, `FAULTED` -> `RUNNING` (recovery) |
| `WorkerLifecycleOrchestrator` | class | Discovers all `WorkerRuntime` beans, calls `initialize()` at startup (`@Priority(APPLICATION + 10)`), `shutdown()` at `@PreDestroy`. Sequential across types, fail-open per worker |
| `WorkerCapabilityResolver<T>` | interface (SPI) | Tenancy-aware endpoint resolution: `resolve(tag, tenancyId)`, `firstMatch(capabilities, tenancyId)`, `capabilities()`, `canResolve(tag, tenancyId)` |
| `WorkerCorrelationContext` | record | Per-dispatch context: `caseInstance`, `worker`, `idempotency`, `tenancyId`, `bindingName` |
| `PendingCompletion` | record | Registry entry per async dispatch: `dispatchId`, `workerType`, `faultAddress`, `correlationContext`, `callbackToken`, `capability`, `eventLogId`, `registeredAt`, `expiresAt`, `provisionerMeta` |
| `AsyncWorkerCompletionRegistry` | class | In-memory `ConcurrentHashMap` pending completion store. `expireStale()` fires `CompletionExpiredEvent` CDI async. Schedule: `casehub.workers.async.expiry-check-interval` (default 5m) |
| `WorkerCallbackResource` | class (JAX-RS) | `POST /workers/complete/{dispatchId}` -- REST callback for external systems. Token validation, re-registration on mismatch |
| `WorkerCompletionPayload` | record | Callback request body: `output` (Map), `faulted` (boolean), `errorMessage` (String) |
| `WorkflowCompletionPublisher` | class | Fires `WorkflowExecutionCompleted.approved()` on `WORKER_EXECUTION_FINISHED` via `eventBus.publish()` |
| `WorkerStatusPublisher` | class | Delegates to `WorkerStatusListener`: `onWorkerStarted`, `onWorkerCompleted`, `onWorkerStalled` |
| `WorkerFaultEvent` | record | Vert.x event bus payload: `caseInstance`, `worker`, `capability`, `inputDataHash`, `eventLogId`, `cause`, `bindingName` |
| `WorkerFaultPublisher` | class | Generic fault publisher -- two overloads: from explicit params, from `PendingCompletion` |
| `WorkerFaultHandler` | class | Shared fault handler body: persist -> check permanent -> count -> backoff -> retry-or-exhaust |
| `WorkerRetrySupport` | class | Shared retry building blocks -- static: `resolveRetryPolicy`, `computeBackoffDelayMs`, `parseRetryAfter`. Instance: `persistFailureLog`, `countFailedAttempts`, `publishRetriesExhausted` |
| `CompletionExpiredEvent` | record | CDI async event: `pending` |
| `FaultCallbackEvent` | record | CDI async event: `pending`, `cause` |
| `WorkerCompletionExpiryObserver` | class | `@ObservesAsync CompletionExpiredEvent` -> fault publisher |
| `WorkerFaultCallbackObserver` | class | `@ObservesAsync FaultCallbackEvent` -> fault publisher |
| `PermanentFaultException` | class | Non-retryable fault signal. Has `statusCode` field |
| `RetryAfterException` | class | Retryable with explicit `retryAfterMs` delay |
| `WorkerProvisionerSupport` | class (utility) | Static: `validateCapabilities(requested, supported)`, `wrap(Throwable, capability)` |
| `WorkerProvisioningException` | class | Provisioning failure. Static factory: `noRouteFound(capabilities)` |
| `CasehubWorkerHeaders` | class (constants) | Header names: `casehub-worker-id`, `casehub-idempotency`, `casehub-case-id`, `casehub-tenancy-id`, `casehub-task-type`, `casehub-callback-token`, `casehub-work-status` |

### workers-http

9 classes in `io.casehub.workers.http`:

| Type | Purpose |
|------|---------|
| `HttpWorkerRuntime` | `WorkerRuntime` impl. Initializes `HttpEndpointResolver` |
| `HttpEndpointResolver` | `WorkerCapabilityResolver<ResolvedEndpoint>`. 3-tier resolution: SPI `HttpWorkerRoute` beans, config properties, `EndpointRegistry`. Config prefix: `casehub.workers.http.endpoints.{tag}.*` |
| `HttpWorkerExecutionManager` | `WorkerExecutionManager` with `@WorkerBackend`. Dispatches via Vert.x `WebClient`. Sync: send + handle response. Async: register `PendingCompletion`, send with callback headers |
| `HttpWorkerRoute` | SPI interface for Tier 1 endpoint registration: `capabilityTag()`, `url()`, `method()` (default POST), `exchangeMode()` (default SYNC), `headers()`, `timeoutSeconds()` |
| `ResolvedEndpoint` | Record: `url`, `method`, `mode` (ExchangeMode), `headers`, `timeoutSeconds` |
| `ExchangeMode` | Enum: `SYNC`, `ASYNC` |
| `HttpWorkerConstants` | `WORKER_TYPE = "http"` |
| `HttpWorkerEventBusAddresses` | `HTTP_WORKER_FAULT` address constant |
| `HttpWorkerFaultEventHandler` | Fault stub -> `WorkerFaultHandler` |

Key implementation details:
- URI template interpolation: `{fieldName}` placeholders resolved from `inputData`. Missing keys -> `PermanentFaultException`
- HTTP 4xx (except 429) -> `PermanentFaultException`
- HTTP 429 with `Retry-After` header -> `RetryAfterException`. In async mode, capped to remaining TTL of `PendingCompletion`
- Response body: deserialized as `Map<String, Object>` via Jackson. Empty/unparseable -> empty map
- WebClient created `@PostConstruct`, not per-request

### workers-camel

10 classes across two packages:

| Type | Package | Purpose |
|------|---------|---------|
| `CamelWorkerRuntime` | `.camel` | `WorkerRuntime` impl. Initializes `CamelCapabilityResolver` |
| `CamelCapabilityResolver` | `.camel` | `WorkerCapabilityResolver<String>` (String = Camel URI). 3-tier: SPI `CamelWorkerRoute` beans, config map, convention auto-discovery from `CamelContext.getRoutes()` |
| `CamelWorkerExecutionManager` | `.camel` | `WorkerExecutionManager` with `@WorkerBackend`. Sync via `ProducerTemplate.request()`, async via `ProducerTemplate.send()` + `PendingCompletion` |
| `CamelWorkerRoute` | `.camel` | SPI interface: `capabilityTag()`, `entryUri()`, `exchangePattern()` |
| `CamelWorkerConstants` | `.camel` | `WORKER_TYPE = "camel"` |
| `CamelWorkerEventBusAddresses` | `.camel` | `CAMEL_WORKER_FAULT` address constant |
| `CamelWorkerFaultEventHandler` | `.camel` | Fault stub -> `WorkerFaultHandler` |
| `CasehubComponent` | `.camel.component` | Camel component registered as `casehub:` URI scheme. Creates `CasehubEndpoint` |
| `CasehubEndpoint` | `.camel.component` | Producer-only endpoint. Creates `CasehubProducer`. Consumer creation throws `UnsupportedOperationException` |
| `CasehubProducer` | `.camel.component` | Resolves `PendingCompletion` by `casehub-worker-id` header. Completes or faults based on exchange exception or `casehub-work-status: FAULTED` header |

Key implementation details:
- Convention auto-discovery (Tier 3): iterates `CamelContext.getRoutes()`, registers routes where `routeId` matches `direct:{routeId}` or `direct://{routeId}` endpoint URI. Default pattern: `InOnly` (async)
- `CasehubProducer` obtains CDI beans via `CDI.current().select()` since Camel producers are not CDI-managed
- Sync fault detection: exchange exception OR `casehub-work-status: FAULTED` header (two paths)
- Config: `casehub.workers.camel.capabilities.{tag}={uri}` (simple Map<String,String>)

### workers-mcp

10 classes in `io.casehub.workers.mcp`:

| Type | Purpose |
|------|---------|
| `McpWorkerRuntime` | `WorkerRuntime` impl. Orchestrates server initialization: resolver config load, per-server session init + tool discovery. Parallel init via `Uni.join().all()`. RUNNING if any server succeeds; FAULTED if all fail |
| `McpServerResolver` | `WorkerCapabilityResolver<ResolvedMcpServer>`. Config + EndpointRegistry resolution. Manages `capabilityToServerName` mapping. `registerDiscoveredTools()` for post-discovery tool registration. Config acts as allowlist when `discovery=auto` |
| `McpSessionManager` | Session lifecycle: `getOrInitialize(serverName)` with concurrent dedup via `ConcurrentHashMap.computeIfAbsent` + `Uni.memoize().indefinitely()`. `invalidate(serverName)` on session expiry. Shutdown sends DELETE to server |
| `McpSession` | Session state: `sessionId`, `protocolVersion`, `nextRequestId()` (AtomicLong, starts at 2 after init) |
| `McpWorkerExecutionManager` | `WorkerExecutionManager` with `@WorkerBackend`. Builds JSON-RPC `tools/call` request, sends via WebClient, parses response (JSON or SSE) |
| `ResolvedMcpServer` | Record: `name`, `url`, `timeoutSeconds`, `headers`, `tools` (Set<String>) |
| `ServerInitResult` | Record: `serverName`, `success`, `session`, `discoveredTools`, `error`. Static factories: `success()`, `failure()` |
| `McpWorkerConstants` | `WORKER_TYPE = "mcp"`, `PROTOCOL_VERSION = "2025-06-18"`, `CLIENT_NAME = "CaseHub"`, `CLIENT_VERSION = "0.2"` |
| `McpWorkerEventBusAddresses` | `MCP_WORKER_FAULT` address constant |
| `McpWorkerFaultEventHandler` | Fault stub -> `WorkerFaultHandler` |

Key implementation details:
- Session init critical ordering: `onFailure().invoke(() -> sessions.remove(k))` BEFORE `memoize().indefinitely()`. Reversing caches the failed Uni permanently
- Permanent JSON-RPC error codes: -32600 (Invalid Request), -32601 (Method not found), -32602 (Invalid params), -32700 (Parse error)
- `isError: true` in tool result is retryable -- MCP spec example is "API rate limit exceeded"
- 404 with active `Mcp-Session-Id` -> session expired, invalidate + retryable. 404 without session -> `PermanentFaultException`
- SSE parsing: splits body on `\n\n`, extracts `data:` lines, finds JSON-RPC response matching expected request ID
- Output: `structuredContent` (object) preferred, then `content` (array wrapped in Map), then empty
- Capability tags: `mcp:{serverName}:{toolName}`. Parsed by static `parseServerName()` / `parseToolName()`
- `McpServerResolver.ServerConfig` inner record: `name`, `url`, `tools`, `timeoutSeconds`, `headers`, `discovery`

### workers-github-actions

6 classes in `io.casehub.workers.githubactions`:

| Type | Purpose |
|------|---------|
| `GitHubActionsWorkerRuntime` | `WorkerRuntime` impl. Checks token availability |
| `GitHubActionsWorkerExecutionManager` | `WorkerExecutionManager` with `@WorkerBackend`. Dispatches to GitHub API. `supports()` checks against two fixed capability constants |
| `GitHubActionsTokenResolver` | Per-org + global PAT resolution. `casehub.workers.github-actions.tokens.{org}` -> `casehub.workers.github-actions.token` fallback. Configurable API base URL |
| `GitHubActionsWorkerConstants` | `WORKER_TYPE = "github-actions"`, two capability tag constants |
| `GitHubActionsWorkerEventBusAddresses` | `GITHUB_ACTIONS_WORKER_FAULT` address constant |
| `GitHubActionsWorkerFaultEventHandler` | Fault stub -> `WorkerFaultHandler` |

Key implementation details:
- Fire-and-forget: no `PendingCompletion`, no async registry. 2xx = dispatched, complete immediately
- 422 on `workflow-dispatch` -> `RetryAfterException(60_000)` -- GitHub's trigger definition caching (GE-20260426-805acb). 422 on `repository-dispatch` -> `PermanentFaultException` (malformed request)
- `ref` is required for `workflow-dispatch` -- GitHub API rejects without it
- GitHub API version: `2022-11-28`

### workers-script

7 classes in `io.casehub.workers.script`:

| Type | Purpose |
|------|---------|
| `ScriptWorkerRuntime` | `WorkerRuntime` impl. Initializes `ScriptDefinitionResolver` |
| `ScriptDefinitionResolver` | `WorkerCapabilityResolver<ScriptDefinition>`. Config-driven: `casehub.workers.script.scripts.{name}.*`. Capability tag prefix: `script:` |
| `ScriptWorkerExecutionManager` | `WorkerExecutionManager` with `@WorkerBackend`. Executes via `ProcessBuilder`. Synchronous: blocks on process completion |
| `ScriptDefinition` | Record: `name`, `command`, `args` (List), `workingDirectory`, `environment` (Map), `timeoutSeconds`, `maxOutputBytes` |
| `ScriptWorkerConstants` | `WORKER_TYPE = "script"` |
| `ScriptWorkerEventBusAddresses` | `SCRIPT_WORKER_FAULT` address constant |
| `ScriptWorkerFaultEventHandler` | Fault stub -> `WorkerFaultHandler` |

Key implementation details:
- Input: stdin receives `inputData` as JSON. Broken pipe on stdin (process exited before write) is silently ignored
- Output: JSON object stdout -> structured `Map`. Otherwise -> `{stdout, stderr, exitCode}` wrapper
- Stream draining: dedicated `ExecutorService` (cached thread pool, daemon threads) drains stdout and stderr concurrently. Bounded by `maxOutputBytes` but continues reading past cap to prevent SIGPIPE deadlock
- Timeout -> `PermanentFaultException` (diverges from HTTP/MCP where timeout is retryable -- a subprocess that burned its full timeout will timeout again)
- Non-zero exit -> `RuntimeException` (retryable). Command not found -> `PermanentFaultException`
- Stream executor created `@PostConstruct`, shutdown `@PreDestroy`

### workers-k8s

11 classes in `io.casehub.workers.k8s`:

| Type | Purpose |
|------|---------|
| `K8sWorkerRuntime` | `WorkerRuntime` impl. Validates cluster connectivity, starts informers per namespace. FAULTED if no configs, cluster unreachable, or all informers fail |
| `JobDefinitionResolver` | `WorkerCapabilityResolver<JobDefinition>`. Config-driven: `casehub.workers.k8s.jobs.{name}.*`. Tag prefix: `k8s:`. Capability tag max 63 chars (K8s label limit). Eager init via `@PostConstruct` |
| `K8sWorkerExecutionManager` | `WorkerExecutionManager` with `@WorkerBackend`. Validates input size, registers `PendingCompletion`, builds Job, creates via `KubernetesClient`. Implements `schedulePersistedEvent()` for restart recovery |
| `K8sJobBuilder` | Static utility. Two modes: `buildFromImage()` (constructs Job from scratch) and `buildFromTemplate()` (loads classpath YAML, overlays CaseHub fields). Template overlay: name, namespace, labels (merge, CaseHub wins), backoffLimit, activeDeadlineSeconds, ttlSecondsAfterFinished, restartPolicy (always Never), first container env vars (append) |
| `K8sJobInformerManager` | `SharedIndexInformer<Job>` per namespace. Terminal event handling via `processTerminal()`. Restart recovery via `recoverFromJob()`. Delete handling: terminal Jobs get processed; non-terminal deletions fault. `classifyJobFailure()` with Pod-level enrichment |
| `K8sJobOutputCapture` | Captures Pod logs from last Pod by creation timestamp. JSON object parsing with `{stdout, exitCode}` fallback. Bounded by `maxOutputBytes` |
| `JobDefinition` | Record: `name`, `namespace`, `image`, `command`, `args`, `template`, resource requests/limits, `timeoutSeconds`, `ttlAfterFinished`, `backoffLimit`, `maxOutputBytes`, `serviceAccount`, `labels`, `environment`, `cleanup` |
| `CleanupPolicy` | Enum: `DELETE`, `RETAIN` |
| `K8sWorkerConstants` | `WORKER_TYPE = "k8s"`, `TAG_PREFIX = "k8s:"`, label key constants, annotation key constant |
| `K8sWorkerEventBusAddresses` | `K8S_WORKER_FAULT` address constant |
| `K8sWorkerFaultEventHandler` | Fault stub -> `WorkerFaultHandler` |

Key implementation details:
- `restartPolicy: Never` always enforced; `backoffLimit` defaults to 0 -- CaseHub's fault pipeline owns retry
- Job naming: `casehub-{slug}-{8-char-hex}`. Slug max 40 chars with 5-char hash suffix for truncation
- Labels for recovery: `casehub.io/case-id`, `casehub.io/worker-name`, `casehub.io/event-log-id`, `casehub.io/idempotency`. `bindingName` as annotation (may exceed 63-char label limit)
- Restart recovery: `processTerminal()` calls `recoverFromJob()` when `registry.complete()` returns empty. Reconstructs `WorkerCorrelationContext` from Job labels, loads `CaseInstance` from repository. `recoveredDispatchIds` (ConcurrentHashMap key set) prevents double-completion
- `schedulePersistedEvent()`: queries K8s for existing Jobs by label selector (case ID, capability, worker name). If found, lets informer handle. If not found, re-dispatches via `submit()`
- Eager resolver initialization via `@PostConstruct` eliminates race between engine recovery (`@Priority(22)`) and worker initialization (`@Priority(APPLICATION + 10)`)
- Cleanup: eager delete if `cleanup != RETAIN`, plus `ttlSecondsAfterFinished` safety net (default 600s, minimum 300s enforced by `Math.max(300, ...)`)
- Fault classification: `BackoffLimitExceeded` (with backoffLimit=0, enriches from Pod reason), `DeadlineExceeded` (enriches from Pod waiting state), `OOMKilled`, `ImagePullBackOff`, `ErrImagePull`, `InvalidImageName`, `CreateContainerConfigError` -> permanent. `Evicted`, `Preempting` -> retryable. API 403/404/422 -> permanent. API 409 -> retryable
- Input size validated against `maxInputBytes` config (default 256KB) before Job creation
- Informer events: `onAdd` handles reconnection (processes terminal Jobs seen during initial list). `onUpdate` processes terminal state transitions. `onDelete` of non-terminal Jobs faults as "deleted externally"
- All informer event callbacks dispatch to `Infrastructure.getDefaultWorkerPool()` via `Uni.runSubscriptionOn()` to avoid blocking the informer thread

### workers-testing

1 class in `io.casehub.workers.testing`:

| Type | Purpose |
|------|---------|
| `WorkerTestSupport` | Factory methods: `testCaseInstance()`, `testCaseInstance(tenancyId)`, `testWorker(name, capabilityNames...)`, `testCapability(tag)`. **Test scope only.** |

## Key Invariants

- Workers are stateless -- all state in the case instance or external system, never in execution manager beans
- `tenancyId` propagated through all resolution and dispatch calls
- Completion fires `eventBus.publish()` on `WORKER_EXECUTION_FINISHED` -- never `request()`. Two consumers exist; `publish()` delivers to both
- Retry logic: `failureCount < retryPolicy.maxAttempts()` (strict `<`); null policy defaults to `new RetryPolicy()` (3 attempts, 10s FIXED)
- All fault event handlers use `@ConsumeEvent(blocking = true)` -- fault handling runs on the Vert.x worker pool, not the event loop
- Worker runtime status reflects initialization outcome only -- post-init dispatch failures go through the per-dispatch fault pipeline
- FAULTED -> RUNNING recovery: calling `initialize()` on a FAULTED runtime retries initialization
- Build order: `workers-common` must be first in parent POM `<modules>` -- all others depend on it
- `submit()` 5-arg overload delegates to 6-arg with `bindingName = null` in every module

## Cross-Repo Dependencies

| Dependency | Why |
|---|---|
| `casehub-worker-api` | `Worker`, `Capability`, `WorkerFunction`, `WorkerResult`, `WorkResult` |
| `casehub-engine-api` | `WorkerStatusListener`, `CaseHubEventType`, `EventStreamType` |
| `casehub-engine-common` | `WorkerExecutionManager`, `WorkerBackend`, `WorkflowExecutionCompleted`, `WorkerRetriesExhaustedEvent`, `CaseInstance`, `EventLog`, `EventLogRepository`, `CaseInstanceRepository`, `EventBusAddresses`, `WorkerExecutionKeys` |
| `casehub-platform-api` | `EndpointRegistry`, `EndpointDescriptor`, `EndpointProtocol`, `EndpointPropertyKeys`, `Path`, `RetryPolicy`, `BackoffStrategy`, `ExecutionPolicy` |

## Depended On By

| Repo | What it uses |
|------|-------------|
| Application-tier repos that need worker dispatch | Add specific worker modules as classpath dependencies; `WorkerLifecycleOrchestrator` auto-discovers them |

## Current State

- All 8 modules (common, http, camel, github-actions, mcp, script, k8s, testing) on main with tests
- Consistent four-class pattern across all dispatch modules (Runtime, Resolver, ExecutionManager, FaultEventHandler)
- `@WorkerBackend` qualifier enables CDI-based dynamic dispatch
- MCP module supports Streamable HTTP transport with configurable `tools/list` discovery (`discovery=auto|manual`)
- Camel module includes a custom `casehub:` Camel component for in-route completion signalling, plus convention-based auto-discovery from `CamelContext` routes
- K8s module supports restart recovery via enriched Job labels, eager resolver initialization, and `schedulePersistedEvent()` for engine recovery
- `AsyncWorkerCompletionRegistry` with TTL-based expiry for async dispatch patterns (HTTP async, Camel InOnly, K8s)
- `WorkerFaultHandler` with configurable retry, backoff (FIXED/EXPONENTIAL/EXPONENTIAL_WITH_JITTER), `RetryAfterException` support, and permanent fault detection
- Reactive tier fully retired: no provisioners, all `submit()` methods return `void`

## Open Issues

- `#16` -- multi-cluster support for workers-k8s (open)
- `#13` -- CI integration (open, partially addressed)

## Design Documents

- `docs/superpowers/specs/2026-06-08-casehub-workers-camel-design.md` -- Camel worker + workers-common infrastructure design (v7, 6 review cycles)
- `docs/superpowers/specs/2026-06-09-casehub-workers-http-design.md` -- HTTP worker design
- `docs/superpowers/specs/2026-06-12-casehub-workers-mcp-design.md` -- MCP worker design
- `docs/superpowers/specs/2026-06-10-casehub-workers-github-actions-design.md` -- GitHub Actions worker design
- `docs/superpowers/specs/2026-06-16-casehub-workers-script-design.md` -- Script worker + fault pipeline extraction design
- `docs/superpowers/specs/2026-07-01-casehub-workers-k8s-design.md` -- K8s Job worker design
- `docs/superpowers/specs/2026-07-03-k8s-restart-recovery-design.md` -- K8s restart recovery design
- `docs/superpowers/specs/2026-06-13-worker-runtime-lifecycle-mcp-discovery-design.md` -- Runtime lifecycle SPI + MCP discovery design
- `docs/superpowers/specs/2026-06-18-endpoint-registry-wiring-design.md` -- EndpointRegistry wiring design
- `docs/superpowers/specs/2026-07-06-binding-name-propagation-design.md` -- bindingName propagation design
- `docs/superpowers/specs/2026-06-25-migrate-worker-api-imports-design.md` -- Worker API migration design
- `docs/adr/0001-worker-runtime-spi-placement.md` -- ADR: WorkerRuntime SPI in workers-common, not engine-api
- `ARC42STORIES.MD` -- primary architecture record; sections 9-10 for layer details, anti-patterns, and gotchas
