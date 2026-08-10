# Worker Runtime Lifecycle + MCP Dynamic Tool Discovery

**Issue:** casehubio/casehub-workers#7
**Date:** 2026-06-13
**Status:** Approved (revised after review)

---

## Problem

The MCP worker (v1) requires every tool to be declared explicitly in config:

```properties
casehub.workers.mcp.servers.slack.tools=send-message,list-channels
```

MCP servers already expose their tools via `tools/list` — a protocol-level discovery mechanism. Operators shouldn't need to enumerate every tool when the server can tell us.

Beyond MCP, all four worker types independently wire their own startup and shutdown using ad-hoc CDI patterns (`@Observes StartupEvent`, `@PreDestroy`). There is no common worker lifecycle — no shared way to initialize, discover capabilities, report status, or shut down.

## Solution

Two changes:

1. **Worker Runtime Lifecycle SPI** — a common lifecycle interface in `workers-common` that all worker types implement.
2. **MCP Dynamic Tool Discovery** — the MCP worker's lifecycle implementation calls `tools/list` at startup to auto-register tools.

---

## 1. Worker Runtime SPI (workers-common)

### Placement

Both types in `io.casehub.workers.common` — not in `casehub-engine-api`.

The engine-api SPIs (`ReactiveWorkerProvisioner`, `WorkerExecutionManager`, `WorkerStatusListener`) are engine-facing — the engine discovers and consumes them. `WorkerRuntime` is not consumed by the engine. Its only consumer is `WorkerLifecycleOrchestrator` in `workers-common`. Placing the SPI where it is consumed keeps the dependency direction clean and eliminates cross-repo coordination.

If a future engine component or health endpoint needs `WorkerRuntimeStatus`, extract it at that point. No hypothetical consumers.

### WorkerRuntimeStatus

```java
package io.casehub.workers.common;

/**
 * Lifecycle status of a {@link WorkerRuntime}.
 *
 * <p>Aligned with the status vocabulary used across CaseHub and Serverless
 * Workflow 1.0 where applicable. Workflow/task instances use
 * {@code WorkflowStatus} (PENDING, RUNNING, WAITING, COMPLETED, FAULTED,
 * CANCELLED, SUSPENDED); worker runtimes use a subset appropriate to an
 * executor lifecycle rather than a task instance lifecycle.
 *
 * <p>Current states cover the initialization and shutdown lifecycle.
 * Future states may include:
 * <ul>
 *   <li>{@code SUSPENDED} — temporarily not accepting dispatches (e.g.,
 *       backpressure, maintenance window). Transitions: RUNNING → SUSPENDED
 *       → RUNNING.</li>
 *   <li>{@code DRAINING} — no new dispatches accepted, in-flight work
 *       completing before shutdown. Transition: RUNNING → DRAINING →
 *       STOPPED.</li>
 * </ul>
 *
 * <p>When adding states, preserve the convention: states that accept new
 * dispatches are "active" (currently only {@code RUNNING}); states that
 * reject new dispatches are "inactive" ({@code PENDING}, {@code FAULTED},
 * {@code STOPPED}).
 */
public enum WorkerRuntimeStatus {
    /** Configured but not yet initialized. Initial state. */
    PENDING,
    /** Initialized and accepting dispatches. */
    RUNNING,
    /** Initialization failed or a runtime error made the worker unavailable. */
    FAULTED,
    /** Shutdown completed. Terminal state. */
    STOPPED
}
```

State machine:

```
PENDING → RUNNING → STOPPED
PENDING → FAULTED → STOPPED
FAULTED → RUNNING (via re-initialization)
```

### WorkerRuntime

```java
package io.casehub.workers.common;

import io.smallrye.mutiny.Uni;
import java.util.Set;

// Note: Uni is used by initialize() and shutdown().
// capabilities() returns Set<String> synchronously.

/**
 * Lifecycle contract for a worker runtime — the infrastructure that executes
 * dispatched work for a specific worker type.
 *
 * <p>A {@code WorkerRuntime} is an <em>executor</em>, not a task instance.
 * It boots, discovers what it can execute, accepts dispatches, and eventually
 * shuts down. Contrast with {@code WorkerStatusListener}, which tracks
 * individual dispatch (task-instance) lifecycle events
 * ({@code onWorkerStarted}, {@code onWorkerCompleted}, {@code onWorkerStalled}).
 *
 * <h3>Relationship to other SPIs</h3>
 * <ul>
 *   <li>{@code ReactiveWorkerProvisioner} — capability probe at case
 *       planning time. Provisioner implementations delegate to their
 *       module's resolver, which is populated during
 *       {@link #initialize()}. The provisioner does not call
 *       {@link #capabilities()} directly.</li>
 *   <li>{@code WorkerExecutionManager} — dispatch at execution time. Uses
 *       the worker module's internal resolver (e.g., {@code McpServerResolver})
 *       to route a capability tag to a concrete target.</li>
 *   <li>{@code WorkerStatusListener} / {@code ReactiveWorkerStatusListener}
 *       — per-dispatch status callbacks. Orthogonal to runtime lifecycle.</li>
 * </ul>
 *
 * <h3>Terminology alignment</h3>
 * <p>The status vocabulary draws from Serverless Workflow 1.0
 * ({@code WorkflowStatus}) and CaseHub's own {@code CaseStatus} /
 * {@code PlanItemStatus}. Where a concept maps directly (PENDING, RUNNING,
 * FAULTED), the same name is used. Where worker runtimes have concerns
 * that task instances do not (capability discovery, connection pooling,
 * session management), worker-specific terms are introduced.
 *
 * <h3>Future lifecycle methods</h3>
 * <p>Methods that may be added as consumers emerge:
 * <ul>
 *   <li>{@code suspend()} / {@code resume()} — RUNNING ↔ SUSPENDED,
 *       for backpressure or maintenance windows.</li>
 *   <li>{@code healthCheck()} — liveness/readiness probe, returning
 *       current status plus diagnostics.</li>
 *   <li>{@code drain()} — stop accepting new dispatches, wait for
 *       in-flight work to complete, then transition to STOPPED.</li>
 * </ul>
 *
 * <h3>Implementation notes</h3>
 * <p>Implementations must be {@code @ApplicationScoped}. The runtime
 * orchestrator discovers all {@code WorkerRuntime} beans via CDI and
 * calls {@link #initialize()} at application startup. Implementations
 * must be safe to call from the Vert.x event loop — avoid blocking
 * operations or use {@code emitOn(Infrastructure.getDefaultWorkerPool())}
 * where necessary.
 */
public interface WorkerRuntime {

    /**
     * Worker type discriminator — e.g., {@code "mcp"}, {@code "http"},
     * {@code "camel"}, {@code "github-actions"}. Must match the value
     * used in {@code PendingCompletion.workerType()} and CDI event
     * filtering.
     */
    String workerType();

    /**
     * Current lifecycle status. Reflects initialization outcome and
     * shutdown state only. Post-initialization failures (server
     * unreachability, connection errors) are handled by the per-dispatch
     * fault pipeline and do not change runtime status. A future
     * {@code healthCheck()} method could surface runtime-level
     * degradation.
     *
     * @see WorkerRuntimeStatus
     */
    WorkerRuntimeStatus status();

    /**
     * Boot the worker runtime: load configuration, establish connections,
     * discover capabilities.
     *
     * <p>Transitions: {@code PENDING → RUNNING} on success,
     * {@code PENDING → FAULTED} on failure. Calling {@code initialize()}
     * on a runtime that is already {@code RUNNING} is a no-op. Calling
     * {@code initialize()} on a {@code FAULTED} runtime retries
     * initialization (enabling recovery without application restart).
     *
     * <p>For workers with external connectivity (e.g., MCP session
     * initialization, remote endpoint health checks), this method
     * performs the initial handshake. Capability discovery (e.g.,
     * MCP {@code tools/list}) also happens inside this method.
     * After {@code initialize()} completes, {@link #capabilities()}
     * returns the full set.
     */
    Uni<Void> initialize();

    /**
     * Release resources held by this worker runtime: close sessions,
     * return connections, cancel timers.
     *
     * <p>Transitions to {@code STOPPED} on completion. Called at
     * application shutdown. Implementations should be best-effort —
     * log failures but do not throw.
     */
    Uni<Void> shutdown();

    /**
     * Returns the set of capability tags this worker can handle.
     *
     * <p>Valid after {@link #initialize()} succeeds. The orchestrator
     * calls this after initialization to log discovered capabilities.
     * Provisioner implementations typically delegate to their module's
     * resolver (e.g., {@code McpServerResolver.capabilities()}) which
     * is populated during {@link #initialize()}.
     *
     * <p>For config-driven workers this returns the statically
     * configured set. For discovery-capable workers (e.g., MCP via
     * {@code tools/list}), this returns dynamically discovered
     * capabilities merged with any config-declared ones.
     *
     * <p>Synchronous — reads from an in-memory map populated during
     * {@link #initialize()}. Consistent with {@link #workerType()} and
     * {@link #status()} which are also synchronous state queries.
     */
    Set<String> capabilities();
}
```

Note: `capabilities()` returns `Set<String>` (synchronous), not `Uni<Set<String>>`. Every implementation reads from an in-memory HashMap populated during `initialize()` — no I/O, no blocking. `workerType()` and `status()` are also synchronous getters. Wrapping in Uni would add allocation cost with no functional benefit. If a future implementation needs async capability queries, the return type can be changed — all implementations are in this repo.

---

## 2. Lifecycle Orchestrator (workers-common)

`WorkerLifecycleOrchestrator` in `io.casehub.workers.common`:

- `@ApplicationScoped`, observes `@Priority(APPLICATION + 10) StartupEvent`.
- Discovers all `WorkerRuntime` beans via `@Inject @Any Instance<WorkerRuntime>`.
- Calls `initialize()` on each. Workers that fail go FAULTED — logged, not fatal to other workers.
- Calls `capabilities()` on workers that reach RUNNING — logs discovered capabilities at INFO.
- `@PreDestroy` calls `shutdown()` on all workers with status != PENDING. Each transitions to STOPPED.
- Sequential initialization across worker types (not parallel). Startup is not a hot path; simplicity and debuggability matter more. Initialization order across worker types is undefined (CDI `Instance` does not guarantee iteration order). No runtime may depend on another runtime's initialization having completed.
- If no `WorkerRuntime` beans are discovered (no worker modules on classpath), the orchestrator logs at INFO and returns.

### Priority

`APPLICATION + 10` ensures the orchestrator fires after any `@Priority(APPLICATION)` observers. During migration, this avoids conflicts with existing `@Observes StartupEvent` methods on resolvers. Once those methods are removed, the priority is simply "after CDI construction, before case processing."

---

## 3. MCP Dynamic Tool Discovery

### Design Shift: Lazy to Eager Session Initialization

The v1 MCP spec (§5.1) established lazy session initialization — sessions are established on first dispatch, cached, and reused. This spec shifts to eager initialization at startup because:

1. `tools/list` requires an initialized session (session ID, protocol version).
2. Capability discovery must complete before any dispatch — the provisioner needs the full capability set at case planning time.

The lazy infrastructure stays intact. `McpSessionManager.getOrInitialize()` remains the entry point. The runtime pre-warms the session cache at startup by calling `getOrInitialize()` for each configured server during `initialize()`. The execution manager continues to call `getOrInitialize()` on dispatch (hitting the cache or re-initializing after 404 invalidation).

### Discovery Flow

After MCP session initialization succeeds (`initialize` + `notifications/initialized`), `McpWorkerRuntime` calls `tools/list` on each configured server:

```json
{"jsonrpc": "2.0", "id": <next>, "method": "tools/list"}
```

Response (per MCP spec):
```json
{
  "jsonrpc": "2.0",
  "id": <id>,
  "result": {
    "tools": [
      {"name": "send-message", "description": "...", "inputSchema": {...}},
      {"name": "list-channels", "description": "...", "inputSchema": {...}}
    ]
  }
}
```

Parses `result.tools[*].name`. Same dual-response parsing (JSON + SSE) as `tools/call`.

### Per-Server Initialization: Parallel Within MCP Runtime

The orchestrator initializes worker types sequentially. Within `McpWorkerRuntime.initialize()`, configured servers are initialized in parallel with per-server error isolation.

Individual server initialization failures are isolated — each server's init Uni catches its own failures and produces a result object (success with session, or failure with cause). Concretely: each server's init Uni is wrapped via `onFailure().recoverWithItem(err -> ServerInitResult.failure(server, err))` so it always succeeds from Mutiny's perspective. The runtime then collects all results via `Uni.join().all().andFailFast()` (which never fails because every element Uni is guaranteed to succeed), inspects the results to count successes and failures, logs faulted servers, and determines aggregate status: RUNNING if at least one server succeeded, FAULTED if all failed.

This matters for deployments with multiple MCP servers: 3 servers at 10s each takes ~10s in parallel vs 30s sequentially. And partial failure is correctly handled — server A timing out does not prevent server B from being available.

### Discovery Modes

Per-server config property:

```properties
casehub.workers.mcp.servers.slack.discovery=auto     # default — call tools/list
casehub.workers.mcp.servers.slack.discovery=manual   # config-only, no tools/list
```

- **`auto` (default):** Calls `tools/list` after session init. If `tools` config property is also present, it acts as an allowlist (see below). If `tools` is absent, all discovered tools are registered.
- **`manual`:** No `tools/list` call. Only config-declared tools are registered. This is v1 behaviour, preserved for servers where operators want explicit control.

### tools Config as Allowlist

When `discovery=auto` and `tools` is also specified:

```properties
casehub.workers.mcp.servers.slack.url=https://slack.internal/mcp
casehub.workers.mcp.servers.slack.tools=send-message,list-channels
```

The `tools` property becomes an allowlist:

- **Register the full config set** — all config-declared tools are registered regardless of what `tools/list` returns.
- **Validate against discovery** — if a config-declared tool is not found in the `tools/list` response, log a warning. The registration is kept (trust the operator). If the tool genuinely doesn't exist, the fault surfaces at dispatch time with a clear JSON-RPC `-32601 Method not found` error.
- **Ignore extra discovered tools** — tools found by `tools/list` but not in the config set are not registered. The operator explicitly chose to restrict exposure.

When `discovery=auto` and `tools` is absent: register all discovered tools.

### Capability Tag Format

Unchanged: `mcp:<server>:<tool>`. A tool named `send-message` on server `slack` becomes `mcp:slack:send-message`. Same format whether the tool was config-declared or discovered.

### Naming Collision

Two servers exposing the same tool name produce distinct tags because the server name is part of the tag: `mcp:slack:send-message` vs `mcp:jira:send-message`. No collision possible at the tag level.

Within a single server, the `tools` config property acts as an allowlist when present — only matching discovered tools are registered.

### Failure Handling

- **Session init fails:** Server is marked faulted. No `tools/list` call attempted. Other servers unaffected.
- **`tools/list` returns error:** Falls back to config-declared tools only. If no tools are config-declared, that server contributes zero capabilities. Warning logged.
- **`tools/list` returns empty tools array:** Valid — server has no tools. Warning logged.
- **All servers fail:** `McpWorkerRuntime` status = FAULTED.
- **Partial failure:** Some servers running, some faulted. `McpWorkerRuntime` status = RUNNING (at least one server works). Faulted servers logged with details.

### McpServerResolver Changes

`McpServerResolver` gains a new method:

```java
void registerDiscoveredTools(String serverName, Set<String> discoveredToolNames)
```

Called by `McpWorkerRuntime` after `tools/list` succeeds. Merge semantics:

- **`tools` config present (allowlist mode):** Register `configTools` (the full config set). Log a warning for each config tool not found in `discoveredToolNames`. Ignore discovered tools not in `configTools`.
- **`tools` config absent (full discovery mode):** Register `discoveredToolNames` (the full discovered set).
- **Both cases:** Create a new `ResolvedMcpServer` record with the merged tool set (since `ResolvedMcpServer` is a record with immutable `Set.copyOf()` tools), replace the entry in `serversByName`, rebuild capability tag mappings in `capabilityToServerName`.

---

## 4. Existing Worker Migration

All four worker types implement `WorkerRuntime`. Their ad-hoc CDI lifecycle methods are removed.

### MCP Worker (McpWorkerRuntime)

- `initialize()`: loads server config (from `McpServerResolver.onStartup()`), initializes sessions in parallel via `McpSessionManager.getOrInitialize()` + `Uni.join().all()`, calls `tools/list` per server (new), registers discovered tools via `McpServerResolver.registerDiscoveredTools()`.
- `shutdown()`: sends DELETE for active sessions (from `McpSessionManager.@PreDestroy`). Transitions to STOPPED.
- `capabilities()`: returns `McpServerResolver.capabilities()`.
- Status: PENDING → RUNNING after at least one server initializes. FAULTED if all servers fail.

### HTTP Worker (HttpWorkerRuntime)

- `initialize()`: scans SPI routes + config endpoints (from `HttpEndpointResolver.onStartup()`).
- `shutdown()`: no-op. Transitions to STOPPED.
- `capabilities()`: returns `HttpEndpointResolver.capabilities()`.
- Status: PENDING → RUNNING.

### Camel Worker (CamelWorkerRuntime)

- `initialize()`: scans SPI routes + config + CamelContext routes (from `CamelCapabilityResolver.onStartup()`).
- `shutdown()`: no-op. Transitions to STOPPED.
- `capabilities()`: returns `CamelCapabilityResolver.capabilities()`.
- Status: PENDING → RUNNING.

### GitHub Actions Worker (GitHubActionsWorkerRuntime)

- `initialize()`: validates token configuration.
- `shutdown()`: no-op. Transitions to STOPPED.
- `capabilities()`: returns configured capability tags.
- Status: PENDING → RUNNING if tokens resolve. FAULTED if no tokens found.

### Resolver Changes

Each resolver loses its `@Observes StartupEvent onStartup()` method. The `initialize()` method becomes package-private, called by the corresponding `WorkerRuntime`. Resolvers remain `@ApplicationScoped` — they are the routing layer (capability tag → concrete target). The lifecycle layer calls into them, not the other way around.

### Provisioner Changes

`ReactiveWorkerProvisioner.getCapabilities()` implementations continue to delegate to their resolver's `capabilities()` method. No functional change — the capabilities set is the same, it's just populated via the lifecycle now.

### terminate() Signature Fix

`McpReactiveWorkerProvisioner.terminate(String workerId)` does not match the current engine-api interface `ReactiveWorkerProvisioner.terminate(String workerId, String tenancyId)`. All four provisioners must be updated to include the `tenancyId` parameter. Not caused by this spec but will surface during implementation.

---

## 5. Scope

Everything happens in `casehub-workers`. No cross-repo coordination needed.

### Changes by Module

- **`workers-common`:** Add `WorkerRuntimeStatus` enum, `WorkerRuntime` interface, `WorkerLifecycleOrchestrator`.
- **`workers-mcp`:** Add `McpWorkerRuntime` with `tools/list` discovery and parallel server init. Add `McpServerResolver.registerDiscoveredTools()`. Remove `McpServerResolver.onStartup()`. Move `McpSessionManager.@PreDestroy` shutdown logic into `McpWorkerRuntime.shutdown()`.
- **`workers-http`:** Add `HttpWorkerRuntime`. Remove `HttpEndpointResolver.onStartup()`.
- **`workers-camel`:** Add `CamelWorkerRuntime`. Remove `CamelCapabilityResolver.onStartup()`.
- **`workers-github-actions`:** Add `GitHubActionsWorkerRuntime`.
- **All provisioners:** Fix `terminate()` signature to match engine-api (`String workerId, String tenancyId`).
- **Docs:** Update CLAUDE.md and PLATFORM.md to document the lifecycle SPI and MCP discovery.

### Not In Scope

- Composite `WorkerExecutionManager` (engine#461) — orthogonal, required for co-deploying multiple workers
- `EndpointRegistry` integration (platform#73) — orthogonal, HTTP Tier 3
- Dashboard/health endpoint consuming `WorkerRuntimeStatus` — no consumer yet
- `suspend()` / `resume()` / `drain()` lifecycle methods — documented in javadoc as future direction
- MCP `tools/list` pagination (`cursor` parameter) — defer until a server returns paginated results
- MCP tool schema caching (storing `inputSchema` for validation) — defer to a future issue
