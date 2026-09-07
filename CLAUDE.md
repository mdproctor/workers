# CaseHub Workers

## Project Type

type: java

## Repository Role

Integration-tier collection of CaseHub worker implementations. Each module provides `ReactiveWorkerProvisioner` and `WorkerExecutionManager` SPI implementations (from `casehub-engine-api` and `casehub-engine-common`) that allow CaseHub cases to dispatch work to different execution runtimes — HTTP endpoints, Apache Camel routes, shell scripts, Kubernetes Jobs, and more.

**Tier:** Integration (alongside `claudony` and `casehub-openclaw` in the build order)

**Design philosophy:** Thin wrappers — each worker module translates a CaseHub case step dispatch into the target runtime's protocol and fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` when done. No domain logic here.

**Spec:** `docs/specs/2026-06-08-casehub-workers-camel-design.md` — fully approved, 7 review cycles.

## Build Commands

```bash
# Build all modules
mvn --batch-mode install

# Publish to GitHub Packages (CI only — requires GITHUB_TOKEN)
mvn --batch-mode deploy -DskipTests
```

## Module Structure

| Module | Artifact | Root package | Purpose |
|--------|----------|-------------|---------|
| `workers-common` | `casehub-workers-common` | `io.casehub.workers.common` | General async worker infrastructure — shared by all worker types |
| `workers-http` | `casehub-workers-http` | `io.casehub.workers.http` | HTTP/webhook worker — 3-tier endpoint resolution, sync/async dispatch |
| `workers-camel` | `casehub-workers-camel` | `io.casehub.workers.camel` | Apache Camel worker — 300+ connectors |
| `workers-github-actions` | `casehub-workers-github-actions` | `io.casehub.workers.githubactions` | GitHub Actions worker — workflow_dispatch + repository_dispatch |
| `workers-mcp` | `casehub-workers-mcp` | `io.casehub.workers.mcp` | MCP worker — dispatch case steps to MCP server tools via Streamable HTTP |
| `workers-script` | `casehub-workers-script` | `io.casehub.workers.script` | Script worker — dispatch case steps to local subprocesses (shell, Python, JS) |
| `workers-k8s` | `casehub-workers-k8s` | `io.casehub.workers.k8s` | Kubernetes Job worker — dispatch case steps as K8s Jobs via fabric8 client, watch-based completion |
| `workers-scenario` | `casehub-workers-scenario` | `io.casehub.workers.scenario` | Scenario worker — dispatch case steps as scenario executions on casehub-pages via GraphQL |
| `workers-testing` | `casehub-workers-testing` | `io.casehub.workers.testing` | Shared test fixtures — **test scope only, never compile/runtime** |

Sub-packages follow function: `.registry`, `.callback`, `.fault`, `.route`, `.component` as needed within each root package.

**Build order:** `workers-common` must be first in parent POM `<modules>` — all others depend on it.

## Engine Integration — Two SPIs, Two Call Sites

Workers implement two engine SPIs — these are called at different times:

| SPI | Call site | Purpose for Camel |
|-----|-----------|-------------------|
| `ReactiveWorkerProvisioner` | `CaseContextChangedEventHandler.tryProvision()` | Capability probe — validates route exists, returns `ProvisionResult.empty()` |
| `WorkerExecutionManager` | `WorkerScheduleEventHandler` | Actual dispatch — sends exchange, manages completion |

Both are `@ApplicationScoped`. `ReactiveWorkerProvisioner` displaces `NoOpReactiveWorkerProvisioner` when worker beans are present. `WorkerExecutionManager` backends are discovered by `CompositeWorkerExecutionManager` via the `@WorkerBackend` qualifier — no displacement needed.

## workers-common Key Types

| Type | Purpose |
|------|---------|
| `PendingCompletion` | Registry entry per async dispatch — carries `dispatchId`, `workerType`, `faultAddress`, `callbackToken`, `capability`, `eventLogId`. Self-routing: `faultAddress` enables generic observers without per-module filtering |
| `WorkerCorrelationContext` | Per-dispatch context — `CaseInstance`, `Worker`, `idempotency`, `tenancyId`, `bindingName`. `bindingName` is nullable — null means engine falls back to `findMatchingCapabilityBinding()` |
| `AsyncWorkerCompletionRegistry` | In-memory pending completion store; `expireStale()` fires `CompletionExpiredEvent` CDI async |
| `WorkflowCompletionPublisher` | Fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` via `eventBus.publish()` |
| `WorkerCallbackResource` | `POST /workers/complete/{dispatchId}` — REST callback for external systems |
| `WorkerRetrySupport` | Shared retry building blocks — `persistFailureLog`, `countFailedAttempts`, `publishRetriesExhausted`, `resolveRetryPolicy`, `computeBackoffDelayMs`, `parseRetryAfter` |
| `PermanentFaultException` | Worker-agnostic "don't retry" signal — extracted from workers-http |
| `RetryAfterException` | Worker-agnostic "retry after delay" signal — extracted from workers-http |
| `FaultCallbackEvent` | CDI async event fired by `WorkerCallbackResource` on faulted REST callback |
| `CompletionExpiredEvent` | CDI async event fired by `AsyncWorkerCompletionRegistry.expireStale()` |
| `CasehubWorkerHeaders` | Header name constants shared across all worker types |
| `WorkerRuntime` | Lifecycle SPI — `initialize()`, `shutdown()`, `capabilities()`, `status()`. All worker types implement this. Orchestrator discovers beans via CDI |
| `WorkerRuntimeStatus` | `PENDING` → `RUNNING` → `STOPPED`, `PENDING` → `FAULTED` → `STOPPED`, `FAULTED` → `RUNNING` (recovery). Aligned with SW 1.0 vocabulary |
| `WorkerLifecycleOrchestrator` | `@ApplicationScoped` — discovers all `WorkerRuntime` beans, calls `initialize()` at startup (`@Priority(APPLICATION + 10)`), `shutdown()` at `@PreDestroy`. Sequential across types, fail-open per worker |
| `WorkerCapabilityResolver<T>` | Tenancy-aware endpoint resolution SPI — `resolve(capabilityTag, tenancyId)`, `firstMatch(capabilities, tenancyId)`, `capabilities()`. All four worker types implement this. HTTP and MCP add EndpointRegistry as Tier 3; Camel and Script pass tenancyId through |
| `WorkerFaultEvent` | Local fault event record — `caseInstance`, `worker`, `capability`, `inputDataHash`, `eventLogId`, `cause`, `bindingName`. `bindingName` propagated from `WorkerCorrelationContext` via `WorkerFaultPublisher` so the fault handler can thread it through retry re-dispatch and retries-exhausted |
| `WorkerFaultPublisher` | Generic fault publisher — parameterized by fault address. Publishes `WorkerFaultEvent`. Two overloads: `fault(faultAddress, ctx, capability, eventLogId, cause)` and `fault(pending, cause)` |
| `WorkerFaultHandler` | Shared fault handler body — persist → PermanentFaultException check → count → RetryAfterException check → retry-or-exhaust. Always uses `emitOn(workerPool)` before re-dispatch. Uses `event.bindingName()` for retry re-dispatch (6-arg `submit()`) and `publishRetriesExhausted()`. Per-module fault event handlers are 5-line stubs delegating here |
| `WorkerCompletionExpiryObserver` | Generic `@ObservesAsync CompletionExpiredEvent` — routes via `faultAddress` from `PendingCompletion`. Replaces per-module expiry observers |
| `WorkerFaultCallbackObserver` | Generic `@ObservesAsync FaultCallbackEvent` — routes via `faultAddress` from `PendingCompletion`. Replaces per-module callback observers |

## workers-camel Key Types

| Type | Purpose |
|------|---------|
| `CamelWorkerConstants.WORKER_TYPE = "camel"` | workerType discriminator — passed to `register()`, used by CDI observers to filter events |
| `CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT` | Separate fault address from Quartz's `WORKFLOW_EXECUTION_FAILED` |
| `CamelWorkerFaultEventHandler` | `@ConsumeEvent(CAMEL_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` |
| `CamelWorkerRuntime` | `WorkerRuntime` implementation — delegates to `CamelCapabilityResolver.initialize()` |

## workers-http Key Types

| Type | Purpose |
|------|---------|
| `HttpWorkerConstants.WORKER_TYPE = "http"` | workerType discriminator |
| `HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT` | Separate fault address from Camel and Quartz |
| `HttpWorkerRoute` | SPI interface for Tier 1 endpoint registration |
| `HttpEndpointResolver` | 3-tier capability tag → `ResolvedEndpoint` resolution (SPI bean > config > EndpointRegistry). Registry lookup: `Path.of("http", capabilityTag)` with tenancyId. Protocol check: `EndpointProtocol.HTTP` only |
| `HttpWorkerExecutionManager` | Sync/async dispatch via Vert.x WebClient — reactive-native, no `emitOn` needed |
| `HttpWorkerFaultEventHandler` | `@ConsumeEvent(HTTP_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` |
| `ExchangeMode` | `SYNC` (default) or `ASYNC` |
| `HttpWorkerRuntime` | `WorkerRuntime` implementation — delegates to `HttpEndpointResolver.initialize()` |

## workers-github-actions Key Types

| Type | Purpose |
|------|---------|
| `GitHubActionsWorkerConstants.WORKER_TYPE = "github-actions"` | workerType discriminator |
| `GitHubActionsWorkerEventBusAddresses.GITHUB_ACTIONS_WORKER_FAULT` | Separate fault address from HTTP and Camel |
| `GitHubActionsTokenResolver` | Per-org + global PAT resolution from config properties |
| `GitHubActionsWorkerExecutionManager` | Dispatches via Vert.x WebClient — fire-and-forget on 204 |
| `GitHubActionsWorkerFaultEventHandler` | `@ConsumeEvent(GITHUB_ACTIONS_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` |
| `GitHubActionsReactiveWorkerProvisioner` | Capability probe — validates tags and token availability |
| `GitHubActionsWorkerRuntime` | `WorkerRuntime` implementation — validates token config; FAULTED if no token, supports FAULTED → RUNNING recovery |

## workers-mcp Key Types

| Type | Purpose |
|------|---------|
| `McpWorkerConstants.WORKER_TYPE = "mcp"` | workerType discriminator |
| `McpWorkerEventBusAddresses.MCP_WORKER_FAULT` | Separate fault address from HTTP, Camel, and GitHub Actions |
| `McpServerResolver` | Config + discovery + EndpointRegistry server registry — N:1 capability tag mapping (`mcp:<server>:<tool>` → `ResolvedMcpServer`). Resolution: config > EndpointRegistry (Tier 3). Registry lookup: `Path.of("mcp", serverName)` with tenancyId. Protocol check: `EndpointProtocol.MCP` only. `firstMatch()` validates server existence — tool validation deferred to dispatch. `discovery=auto` (default) calls `tools/list`; `discovery=manual` is config-only. `registerDiscoveredTools()` merges discovered tools with config allowlist |
| `McpSessionManager` | `@ApplicationScoped` — MCP session lifecycle: eager init at startup (pre-warmed by `McpWorkerRuntime`), concurrent dedup via memoized Uni, session caching, shutdown via `McpWorkerRuntime.shutdown()` |
| `McpSession` | Per-server runtime state — `sessionId`, `protocolVersion`, `AtomicLong requestIdCounter` |
| `McpWorkerExecutionManager` | Dispatches `tools/call` via Vert.x WebClient — dual response parsing (JSON + SSE), `structuredContent` preferred |
| `McpWorkerFaultEventHandler` | `@ConsumeEvent(MCP_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` |
| `McpReactiveWorkerProvisioner` | Capability probe — validates tag in resolved set, server URL non-blank |
| `McpWorkerRuntime` | `WorkerRuntime` implementation — parallel server init via `Uni.join().all()` with per-server error isolation (`ServerInitResult`), `tools/list` discovery, eager session pre-warming, delegated shutdown |
| `ServerInitResult` | Per-server init outcome record — success (session + discovered tools) or failure (error). Enables partial-failure handling |

## workers-script Key Types

| Type | Purpose |
|------|---------|
| `ScriptWorkerConstants.WORKER_TYPE = "script"` | workerType discriminator |
| `ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT` | Separate fault address from HTTP, Camel, GitHub Actions, and MCP |
| `ScriptDefinition` | Record — `name`, `command`, `args`, `workingDirectory`, `environment`, `timeoutSeconds`, `maxOutputBytes` |
| `ScriptDefinitionResolver` | `WorkerCapabilityResolver<ScriptDefinition>` — config-driven (`casehub.workers.script.scripts.<name>.*`), capability tag prefix `script:` |
| `ScriptWorkerExecutionManager` | Dispatches via `ProcessBuilder` — `runSubscriptionOn(workerPool)`, stdin JSON delivery, bounded stdout/stderr capture, exit code classification. Owns dedicated `ExecutorService` for stream draining (`@PostConstruct`/`@PreDestroy` lifecycle) |
| `ScriptWorkerFaultEventHandler` | `@ConsumeEvent(SCRIPT_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` |
| `ScriptReactiveWorkerProvisioner` | Capability probe — validates tag exists in resolver, command non-blank |
| `ScriptWorkerRuntime` | `WorkerRuntime` implementation — delegates to `ScriptDefinitionResolver.initialize()`. Zero scripts → FAULTED |

## workers-k8s Key Types

| Type | Purpose |
|------|---------|
| `K8sWorkerConstants.WORKER_TYPE = "k8s"` | workerType discriminator. Label constants: `CASE_ID_LABEL`, `WORKER_NAME_LABEL`, `EVENT_LOG_ID_LABEL`, `IDEMPOTENCY_LABEL` for recovery metadata. Annotation constant: `BINDING_NAME_ANNOTATION` — annotation (not label) because `bindingName` is user-defined and may exceed 63-char label limit |
| `K8sWorkerEventBusAddresses.K8S_WORKER_FAULT` | Separate fault address from other workers |
| `JobDefinition` | Config record — `name`, `namespace`, `image`/`template`, `command`, `args`, `cpuRequest`, `cpuLimit`, `memoryRequest`, `memoryLimit`, `timeoutSeconds`, `ttlAfterFinished`, `backoffLimit`, `maxOutputBytes`, `serviceAccount`, `labels`, `environment`, `cleanup` |
| `CleanupPolicy` | `DELETE` (default) / `RETAIN` enum — eager delete + TTL safety net vs. manual cleanup |
| `JobDefinitionResolver` | `WorkerCapabilityResolver<JobDefinition>` — config-driven (`casehub.workers.k8s.jobs.<name>.*`), capability tag prefix `k8s:`, single-tier. `@PostConstruct` eager initialization from Config — eliminates race between engine recovery and worker initialization |
| `K8sJobBuilder` | Static utility — builds fabric8 `Job` from `JobDefinition` + dispatch context. Two paths: image-based (full spec from config fields) and template-based (classpath YAML + overlay). Enforces `restartPolicy: Never`, unique name (`casehub-{slug}-{8-char-hex}`), CaseHub labels + env vars. Adds `bindingName` as annotation when non-null |
| `K8sJobOutputCapture` | Reads Pod logs after completion — lists Pods by `job-name` label, selects last Pod (handles `backoffLimit > 0`), bounded read at `maxOutputBytes`, JSON parsing (valid object → structured map; else → raw wrapper `{stdout, exitCode}`) |
| `K8sWorkerExecutionManager` | `@WorkerBackend @Priority(10)` — creates Job via `kubernetesClient.resource(job).create()`, registers in `AsyncWorkerCompletionRegistry`, validates input size against `maxInputBytes`. Execution model: `runSubscriptionOn(Infrastructure.getDefaultWorkerPool())`. Implements `schedulePersistedEvent()` for restart recovery, injects `CaseInstanceRepository` |
| `K8sReactiveWorkerProvisioner` | Capability probe — validates tag exists in resolver |
| `K8sWorkerRuntime` | `WorkerRuntime` implementation — validates K8s connectivity (`kubernetesClient.getApiVersion()`), starts per-namespace informers, FAULTED if no jobs configured or all informers failed |
| `K8sJobInformerManager` | Shared informer lifecycle — `Map<String, SharedIndexInformer<Job>>` per unique namespace. Label selector: `app.kubernetes.io/managed-by=casehub`. Handles `onAdd` (reconnection), `onUpdate` (terminal state), `onDelete` (TTL vs. external deletion). `processTerminal()`: `registry.complete()` → capture Pod logs → publish completion/fault → delete Job (cleanup policy). `recoverFromJob()` for Job-metadata recovery after restart; `recoveredDispatchIds` (at-most-once guard via `ConcurrentHashMap.newKeySet()`). Injects `CaseInstanceRepository`. Full K8s fault classification: `BackoffLimitExceeded`, `DeadlineExceeded` (enriched with Pod waiting state), `OOMKilled`, `ImagePullBackOff`, eviction/preemption (retryable), API errors (403/404/422/409) |
| `K8sWorkerFaultEventHandler` | `@ConsumeEvent(K8S_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` |

## workers-scenario Key Types

| Type | Purpose |
|------|---------|
| `ScenarioWorkerConstants.WORKER_TYPE = "scenario"` | workerType discriminator |
| `ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT` | Separate fault address from other workers |
| `ResolvedScenarioEndpoint` | Record — `name`, `url`, `timeoutSeconds` |
| `ScenarioEndpointResolver` | `WorkerCapabilityResolver<ResolvedScenarioEndpoint>` — 3-tier with EndpointRegistry, tenant-aware. Config: `casehub.workers.scenario.endpoints.<name>.url`. Registry: `Path.of("scenario", name)`, protocol check `EndpointProtocol.SCENARIO` |
| `ScenarioWorkerExecutionManager` | `@WorkerBackend @Priority(10)` — GraphQL dispatch via Vert.x WebClient, async callback completion via `AsyncWorkerCompletionRegistry` |
| `ScenarioWorkerFaultEventHandler` | `@ConsumeEvent(SCENARIO_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` |
| `ScenarioWorkerRuntime` | `WorkerRuntime` implementation — delegates to `ScenarioEndpointResolver.initializeFromConfig()` |

## Key Rules

- `workers-testing` is never a compile or runtime dependency — test scope only.
- Each worker module activates by classpath presence (`@ApplicationScoped`, no config required to enable). All `WorkerExecutionManager` implementations must be annotated `@WorkerBackend @Priority(10)` and implement `supports(String capabilityName, String tenancyId)` — the composite manager discovers backends via this qualifier.
- Workers are stateless — all state in the case instance or external system, never in provisioner beans.
- `tenancyId` propagated through all calls — bind in Repository layer only (PP-20260520-e6a5f0).
- Completion fires `eventBus.publish()` on `WORKER_EXECUTION_FINISHED` — never `request()`. Two consumers exist (`WorkflowExecutionCompletedHandler` + `PlanItemCompletionHandler`); `publish()` delivers to both.
- Worker faults fire on worker-specific addresses (`CAMEL_WORKER_FAULT`, `HTTP_WORKER_FAULT`, `GITHUB_ACTIONS_WORKER_FAULT`, `MCP_WORKER_FAULT`, `SCRIPT_WORKER_FAULT`, `K8S_WORKER_FAULT`, `SCENARIO_WORKER_FAULT`), NOT `WORKFLOW_EXECUTION_FAILED` — Quartz listens on the latter and would double-process.
- Fault pipeline is centralized in workers-common: `WorkerFaultPublisher` (parameterized by address), `WorkerFaultHandler` (shared retry body), `WorkerCompletionExpiryObserver` and `WorkerFaultCallbackObserver` (generic, route via `faultAddress` from `PendingCompletion`). Per-module fault handlers are 5-line stubs.
- `WorkerFaultHandler` always uses `emitOn(Infrastructure.getDefaultWorkerPool())` before re-dispatch — correct for all workers regardless of whether their `submit()` is blocking or reactive. One unnecessary thread hop for reactive workers is negligible on the error path.
- Retry logic via `WorkerRetrySupport`: `failureCount < retryPolicy.maxAttempts()` (strict `<`); null policy defaults to `new RetryPolicy()` (3 attempts, 10s FIXED).
- HTTP 4xx (except 429) throws `PermanentFaultException` — skips retry immediately.
- HTTP 429 with `Retry-After` header throws `RetryAfterException` — overrides configured backoff delay.
- GitHub Actions 422 on `workflow-dispatch` throws `RetryAfterException(60_000)` — workflow_dispatch trigger caching (GE-20260426-805acb). 422 on `repository-dispatch` throws `PermanentFaultException` — malformed request.
- GitHub Actions `ref` is required for `workflow-dispatch` — GitHub API rejects requests without it.
- MCP dispatch is event-loop native via WebClient — no thread hop needed in execution manager.
- MCP `isError: true` is retryable (not permanent) — MCP spec's own example is "API rate limit exceeded."
- MCP malformed responses are retryable — load balancer HTML pages, proxy timeouts are transient.
- MCP 404 with active `Mcp-Session-Id` → session expired, retryable (re-initializes). 404 without session → `PermanentFaultException` (endpoint not found).
- MCP session initialization uses `ConcurrentHashMap.computeIfAbsent` + `Uni.memoize().indefinitely()` — `onFailure().invoke(remove)` BEFORE `memoize()` (GE-20260609-78dc3a).
- MCP protocol version: `2025-06-18` only. No backwards compatibility with `2024-11-05` HTTP+SSE transport.
- MCP required headers: `Accept: application/json, text/event-stream`, `MCP-Protocol-Version`, `Mcp-Session-Id` (when assigned).
- MCP discovery mode: `discovery=auto` (default) calls `tools/list` at startup; `discovery=manual` is config-only (v1 behaviour).
- MCP `tools` config property is an allowlist when `discovery=auto` — config tools are always registered; discovered tools not in config are ignored. Config tools not found in `tools/list` trigger a warning but are kept (trust the operator).
- MCP session initialization is eager at startup (shift from v1 lazy model) — `McpWorkerRuntime.initialize()` pre-warms the session cache. Lazy infrastructure (`getOrInitialize()`) stays for dispatch-time re-init after 404.
- MCP per-server initialization is parallel within the runtime (`Uni.join().all()` with `ServerInitResult` error isolation). Partial failure: RUNNING if at least one server succeeds.
- Script execution uses `runSubscriptionOn(Infrastructure.getDefaultWorkerPool())` — same as Camel's blocking `ProducerTemplate`. Entire ProcessBuilder lifecycle runs on the worker pool.
- Script stdin delivers `inputData` as JSON. Env vars provide dispatch context: `CASEHUB_CASE_ID`, `CASEHUB_TENANCY_ID`, `CASEHUB_CAPABILITY`, `CASEHUB_IDEMPOTENCY`.
- Script stdout parsing: JSON object → structured output map; JSON array, primitive, or invalid JSON → raw wrapper `{stdout, stderr, exitCode}`.
- Script timeout → `PermanentFaultException` (diverges from HTTP/MCP where timeout is retryable). Rationale: subprocess that burned full timeout will timeout again; each retry wastes OS process + thread for full duration.
- Script non-zero exit → `RuntimeException` (retryable). Command not found or working directory missing → `PermanentFaultException`.
- Script stream draining: bounded read loop (8KB chunks, cap at `maxOutputBytes`, drain past cap to prevent SIGPIPE). Dedicated `ExecutorService` for stream draining — `@PostConstruct`/`@PreDestroy` lifecycle on execution manager.
- K8s Job creation: `runSubscriptionOn(Infrastructure.getDefaultWorkerPool())` — fabric8 client API is blocking. Input size validated against `maxInputBytes` (default 256KB); etcd has ~1.5MB object size limit.
- K8s completion model: watch-based via `SharedIndexInformer<Job>` per namespace. Label selector: `app.kubernetes.io/managed-by=casehub`. `onAdd` handles reconnection (Jobs completed during watch disconnect); `onUpdate` handles terminal state; `onDelete` classifies TTL cleanup vs. external deletion.
- K8s fault classification: `BackoffLimitExceeded` with `backoffLimit=0` (default) checks Pod reason; `backoffLimit>0` → permanent (K8s already retried). `DeadlineExceeded` → permanent (enriched with Pod waiting state if Pod never started). `OOMKilled`, `ImagePullBackOff`, `InvalidImageName`, `CreateContainerConfigError` → permanent. Pod eviction, preemption, node failure → retryable. API 403/404/422 → permanent; 409 conflict → retryable.
- K8s cleanup: eager delete in `processTerminal()` if `cleanup != RETAIN`, plus `ttlSecondsAfterFinished` safety net (default 600s, minimum 300s).
- K8s `backoffLimit` defaults to 0 — CaseHub's fault pipeline owns retry logic. With `backoffLimit=0` + `restartPolicy: Never`, any Pod failure surfaces immediately to CaseHub's fault handler.
- K8s Job naming: `casehub-{slug}-{8-char-hex}` (max 57 chars). Slug: lowercase, replace `[^a-z0-9-]` with `-`, collapse consecutive `-`, truncate to 40 chars (if truncated, replace last 5 chars with 5-char hash of pre-truncation slug).
- K8s invariants: `restartPolicy: Never` always enforced; labels `app.kubernetes.io/managed-by=casehub`, `casehub.io/dispatch-id`, `casehub.io/capability`, `casehub.io/tenancy-id`; env vars `CASEHUB_CASE_ID`, `CASEHUB_TENANCY_ID`, `CASEHUB_CAPABILITY`, `CASEHUB_IDEMPOTENCY`, `CASEHUB_INPUT_DATA` (JSON-serialized).
- K8s template overlay order: `metadata.name`, `metadata.namespace`, `metadata.labels` (merge, CaseHub wins), `spec.backoffLimit`, `spec.activeDeadlineSeconds`, `spec.ttlSecondsAfterFinished`, `spec.template.spec.restartPolicy`, first container env vars (append CaseHub env vars).
- EndpointRegistry resolution order: Tier 1 (SPI beans) > Tier 2 (config) > Tier 3 (EndpointRegistry). Single registry call with tenancyId — registry handles tenant → platform-global fallback internally. `capabilities()` stays static (SPI + config only).
- EndpointRegistry path convention: HTTP uses `Path.of("http", capabilityTag)`, MCP uses `Path.of("mcp", serverName)`. Protocol check on descriptors: HTTP resolver accepts `EndpointProtocol.HTTP` only, MCP accepts `EndpointProtocol.MCP` only. Wrong protocol → ignored (returns empty), not faulted.
- MCP firstMatch() for Tier 3: validates server existence via registry lookup, not individual tool existence. Tool validation deferred to dispatch time (same lazy pattern as 404 session recovery).
- Provisioner tenancyId: uses `context.tenancyId()` from `ProvisionContext` (engine#530 shipped). Dispatch path (`submit()`) is fully tenant-aware via `CaseInstance.tenancyId`.
- Worker lifecycle: all workers implement `WorkerRuntime`. `WorkerLifecycleOrchestrator` calls `initialize()` at startup, `shutdown()` at `@PreDestroy`. Initialization order across worker types is undefined.
- Worker runtime status reflects initialization outcome only — post-init dispatch failures go through the per-dispatch fault pipeline, not runtime status.
- FAULTED → RUNNING recovery: calling `initialize()` on a FAULTED runtime retries initialization.
- K8s recovery on restart: `processTerminal()` reconstructs `PendingCompletion` from Job labels when registry is empty. `recoveredDispatchIds` provides at-most-once guard via atomic `add()`. `CaseInstanceRepository.findByUuid()` loads the CaseInstance; `Worker` is reconstructed from labels with `WorkerFunction.NONE`.
- K8s `schedulePersistedEvent()`: checks K8s for existing Job (label selector with case-id, capability, worker-name). If found → voidItem (informer handles). If not found → re-dispatches via `submit()`.
- K8s Job labels carry recovery metadata: `casehub.io/case-id`, `casehub.io/worker-name`, `casehub.io/event-log-id`, `casehub.io/idempotency`. Pre-upgrade Jobs lacking these labels cannot be recovered — they expire via `ttlSecondsAfterFinished`.
- `JobDefinitionResolver` initializes eagerly via `@PostConstruct` — `capabilities()` returns correct results before any startup observer fires. Eliminates race between engine recovery (`@Priority(22)`) and worker initialization (`@Priority(APPLICATION + 10)`).
- `bindingName` propagated end-to-end: `WorkerCorrelationContext.bindingName()` → `WorkflowCompletionPublisher` → `WorkflowExecutionCompleted.bindingName()`. Null until casehubio/engine#676 ships (engine calls 6-arg `submit()`).
- `bindingName` propagated through fault pipeline: `WorkerCorrelationContext` → `WorkerFaultPublisher` → `WorkerFaultEvent.bindingName()` → `WorkerFaultHandler` → retry re-dispatch (6-arg `submit()`) and `publishRetriesExhausted()`.
- K8s `bindingName` uses annotation (`casehub.io/binding-name`), not label — user-defined values may exceed 63-char label limit. Recovery reads from `job.getMetadata().getAnnotations()` (null-safe for pre-upgrade Jobs).
- Scenario dispatch is async: GraphQL mutation fires, callback URL passed to pages, completion arrives via `WorkerCallbackResource`.
- Scenario `inputData` must contain either `scriptName` (fetches YAML from pages library) or `yaml` (raw YAML). Both null → `PermanentFaultException`. Both present → `scriptName` takes precedence.
- Scenario library fetch: `GET /scenario/library/{name}/yaml` on the pages instance (base URL derived from GraphQL endpoint URL by stripping `/graphql` suffix).
- Scenario callback URL: `{casehub.workers.callback-base-url}/workers/complete/{dispatchId}`.
- Scenario endpoint resolution: 3-tier with EndpointRegistry, tenant-aware. `Path.of("scenario", name)`, protocol check `EndpointProtocol.SCENARIO`.
- Worker faults fire on `SCENARIO_WORKER_FAULT` (`casehub.workers.scenario.fault`), same pattern as all other workers.

## Co-deployment

All worker modules can co-deploy on the same classpath. `CompositeWorkerExecutionManager` (engine-runtime) discovers all `@WorkerBackend`-qualified `WorkerExecutionManager` beans and routes via `supports()`. CDI event cross-talk is prevented by `workerType` discriminator in `PendingCompletion` and per-module fault addresses.

## Cross-Repo Dependencies

| Dependency | Why |
|---|---|
| `casehub-worker-api` | `Worker`, `Capability`, `WorkerFunction`, `WorkerResult`, `WorkerOutcome` — Worker Foundation record types |
| `casehub-engine-api` | `ReactiveWorkerProvisioner`, `ProvisionContext`, `ProvisionResult`, `WorkResult`, `CaseHubEventType`, `EventStreamType` |
| `casehub-engine-common` | `WorkerExecutionManager`, `WorkerBackend`, `WorkerExecutionRoutingStrategy`, `WorkflowExecutionCompleted`, `CaseInstance`, `EventLog`, `EventBusAddresses`, `WorkerExecutionKeys`, `EventLogRepository` |
| `casehub-platform-api` | `EndpointRegistry`, `EndpointDescriptor`, `EndpointPropertyKeys`, `EndpointProtocol`, `Path`, `TenancyConstants`, `ExecutionPolicy`, `RetryPolicy`, `BackoffStrategy` |
| ~~engine#461~~ | ~~Composite `WorkerExecutionManager`~~ — shipped, all backends migrated to `@WorkerBackend` |
| ~~engine#530~~ | ~~Add `tenancyId` to `ProvisionContext`~~ — shipped, wired in #15 |
| ~~engine#531~~ | ~~Remove `getCapabilities()` hard gate in `tryProvision()`~~ — shipped, no workers-side changes needed |
| engine#676 | Add `bindingName` parameter to `WorkerExecutionManager.submit()` — default method overload, `CompositeWorkerExecutionManager` routing, `WorkerScheduleEventHandler.submitIfNeeded()` call site |

## Cross-Repo Conventions

Protocols live in `casehub/garden`. Do not write protocol files in this repo.

## Artifact Locations

| Skill | Writes to |
|-------|-----------|
| brainstorming (specs) | `docs/specs/` |
| writing-plans (plans) | workspace `plans/` |
| handover | workspace `HANDOFF.md` |
| idea-log | workspace `IDEAS.md` |
| design-snapshot | workspace `snapshots/` |
| adr | `docs/adr/` |
| write-blog | project | lands in docs/blog/ — promoted at work end `blog/` |

## Routing

| Artifact   | Destination | Notes |
|------------|-------------|-------|
| adr        | project     | lands in `docs/adr/` |
| blog       | project     | lands in `docs/blog/` — promoted at work end |
| specs      | project     | lands in `docs/specs/` |
| plans      | workspace   | |
| design     | project     | journal in workspace `design/`; merge target is project `ARC42STORIES.MD` |
| handover   | workspace   | |

Living docs — check for drift after significant changes:
- `ARC42STORIES.MD` — primary architecture record; check §9–10 after worker module, SPI, or fault pipeline changes

## Work Tracking

**Issue tracking:** enabled
**GitHub repo:** casehubio/workers

## Workspace

**Project repo:** `proj/`
**Workspace:** `wksp/`
**Workspace type:** public

Git discipline — always use explicit paths:
```bash
git -C proj/ ...   # workspace artifacts
git -C proj/ ...          # project artifacts
```
