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
| `workers-common` | `casehub-workers-common` | Shared infrastructure: `WorkerRuntime` SPI, lifecycle orchestrator, async completion registry, fault handling, retry |
| `workers-http` | `casehub-workers-http` | HTTP dispatch: sync/async, config-driven endpoint resolution, `ExchangeMode` (SYNC/ASYNC) |
| `workers-camel` | `casehub-workers-camel` | Apache Camel dispatch: custom `casehub:` URI scheme, 300+ connectors |
| `workers-github-actions` | `casehub-workers-github-actions` | GitHub Actions dispatch: `workflow_dispatch` + `repository_dispatch`, webhook completion |
| `workers-mcp` | `casehub-workers-mcp` | MCP dispatch: Streamable HTTP transport, `tools/list` discovery, session management |
| `workers-script` | `casehub-workers-script` | Script dispatch: local subprocess execution (shell, Python, JS) |
| `workers-k8s` | `casehub-workers-k8s` | Kubernetes Job dispatch: fabric8 client, watch-based completion, restart recovery |
| `workers-testing` | `casehub-workers-testing` | Test fixtures -- **test scope only, never compile/runtime** |

**Activation:** Each worker module activates by classpath presence (`@ApplicationScoped`, no config required to enable). All modules can co-deploy on the same classpath -- `CompositeWorkerExecutionManager` (engine-runtime) discovers all `@WorkerBackend`-qualified execution managers and routes via `supports()`.

## Key Consumer APIs

### WorkerRuntime SPI

Lifecycle contract for a worker runtime -- the infrastructure that executes dispatched work for a specific worker type. Not a task instance, but an executor.

```java
public interface WorkerRuntime {
    String workerType();              // e.g. "mcp", "http", "camel"
    WorkerRuntimeStatus status();     // PENDING, RUNNING, FAULTED, STOPPED
    Uni<Void> initialize();           // PENDING -> RUNNING or FAULTED
    Uni<Void> shutdown();             // -> STOPPED
    Set<String> capabilities();       // valid after initialize()
}
```

Implementations must be `@ApplicationScoped`. The orchestrator discovers all beans via CDI and calls `initialize()` at startup. Post-initialization failures are handled by the per-dispatch fault pipeline -- they do not change runtime status.

### Engine Integration -- Two SPIs

Workers implement two engine SPIs, called at different times:

| SPI | Call site | Purpose |
|-----|-----------|---------|
| `ReactiveWorkerProvisioner` | `CaseContextChangedEventHandler.tryProvision()` | Capability probe -- validates the worker can handle requested capabilities |
| `WorkerExecutionManager` | `WorkerScheduleEventHandler` | Actual dispatch -- sends exchange, manages completion |

Both are `@ApplicationScoped`. `WorkerExecutionManager` implementations declare `@WorkerBackend @Priority(N)` and implement `supports(String capabilityName, String tenancyId)` for CDI-based dynamic dispatch.

### Dispatch Mechanisms

**HTTP** -- Config-driven endpoint resolution. Two exchange modes: `SYNC` (request/response) and `ASYNC` (fire-and-forget with callback URL).

**Camel** -- Custom `casehub:` Camel component for bidirectional integration with Apache Camel routes. `CamelCapabilityResolver` maps capability tags to Camel route URIs.

**MCP** -- Model Context Protocol over Streamable HTTP. Session management with `Mcp-Session-Id` tracking. Capability discovery via `tools/list` JSON-RPC (`discovery=auto` default, `discovery=manual` for config-only).

**GitHub Actions** -- Triggers workflow runs via GitHub API. Async completion via webhook callback to `WorkerCallbackResource`.

**Script** -- Local subprocess execution. Capability tags map to `ScriptDefinition` records (path, args, timeout). Stdin delivers input as JSON; env vars provide dispatch context.

**Kubernetes** -- Job-based dispatch via fabric8 client. Watch-based completion via `SharedIndexInformer`. Restart recovery from enriched Job labels. Supports image-based and template-based Job specs.

### Fault Handling

All worker modules share a centralized fault pipeline in `workers-common`:

- `WorkerFaultHandler` -- retry logic with configurable backoff, `RetryAfterException` support, permanent fault detection
- `PermanentFaultException` -- non-retryable signal (HTTP 4xx except 429, script timeout, K8s `OOMKilled`/`ImagePullBackOff`)
- `RetryAfterException` -- server-specified delay override (HTTP 429 `Retry-After`, GitHub Actions 422)
- Default retry policy: 3 attempts, 10s FIXED backoff

### Async Completion

`AsyncWorkerCompletionRegistry` tracks pending asynchronous dispatches with TTL-based expiry. `WorkerCallbackResource` receives completion callbacks at `POST /workers/complete/{dispatchId}`.

## Dependencies

| Repo | What it provides |
|------|-----------------|
| `casehub-platform` | Governance types (`RetryPolicy`, `BackoffStrategy`), tenancy, `EndpointRegistry` |
| `casehub-worker` | `Worker`, `Capability`, `WorkerFunction`, `WorkerResult` |
| `casehub-engine` | `CaseInstance`, `EventLogRepository`, `WorkerExecutionManager` SPI |
| Quarkus Camel BOM | Camel module only |

## What It Does NOT Do

- Define worker identity, function, or capability vocabulary -- that is `casehub-worker`
- Schedule or orchestrate work -- that is `casehub-engine`
- Provide `WorkerFunction` implementations -- those live in consuming repos (e.g. `AgentWorkerFunction` in engine)
- Manage worker state machines or task instances -- `WorkerRuntime` is an executor lifecycle, not a task instance lifecycle
- Provide a UI or management API for worker configuration -- workers are configured via Quarkus config properties
- Run as a standalone application -- these are library modules consumed by application-tier deployments
