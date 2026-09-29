# CaseHub Camel Worker

Apache Camel routes as CaseHub worker steps. Any Camel route becomes a dispatchable unit of work within a case lifecycle — the case engine handles orchestration, retry, completion tracking, and fault management; the route handles integration logic.

Built on Quarkus 3.32 and Camel Quarkus. Activates by classpath presence (`@ApplicationScoped`) — no configuration required to enable.

## When to use this (and when not to)

Not every Camel route should be a worker. The decision depends on whether the route operates within a managed case lifecycle.

| Situation | Right approach | Why |
|---|---|---|
| Standalone service — no multi-step process context | Deploy as a normal Quarkus/Camel service | A worker adds nothing when there's no case to manage |
| Existing service that a case needs to call | HTTP worker (planned) | CaseHub calls your service via HTTP POST — zero changes to the existing service |
| New integration built for a case flow | **Camel worker** | Route runs inside the CaseHub runtime with direct lifecycle integration |
| Multi-step process with human decisions between automated steps | **Camel worker** for the automated steps | Cases span days/weeks; routes are request-scoped — the case engine bridges the gap |

The honest tradeoff: if your Camel route is a stateless request/response service that will never be part of a multi-step business process, a worker adds overhead without adding value. The value scales with process complexity, human-in-the-loop steps, and the need for case-level visibility.

### What the case engine provides that you stop building

A Quarkus/Camel developer already has good answers for many per-route concerns — `@Retry`, `onException`, `IdempotentConsumer`, MicroProfile Telemetry. Those are real and we won't pretend otherwise. The gap is at the **process level**:

- **Long-lived state across steps** — a Camel route is request-scoped; a case spans days or weeks with human decisions between automated steps. The state lives in the case instance, not in-memory in a route.
- **Process visibility** — "show me all claims stuck at the approval step" is a query against case state. With standalone routes, that's a custom dashboard you build and maintain.
- **Ad-hoc intervention** — skip a step, retry from a specific point, reassign, escalate. These are case management primitives. With routes alone, you're building a bespoke workflow engine.
- **Runtime composability** — a case flow can mix Camel workers, HTTP workers, script workers, and (planned) MCP tool workers. Swap one implementation for another without changing the case definition.
- **Orchestration is externalized** — the route does one thing. The engine decides what runs next. Multi-step sequencing, parallel fan-out, conditional branching — that's the case definition, not Camel EIP wiring between routes.

## Architecture

### How the engine dispatches to Camel

CaseHub workers implement two engine SPIs — called at different points in the case lifecycle:

```
                          Case Step Dispatch
                                 |
                    +------------+------------+
                    |                         |
             tryProvision()              schedule()
                    |                         |
       ReactiveWorkerProvisioner    WorkerExecutionManager
                    |                         |
      CamelReactiveWorkerProvisioner  CamelWorkerExecutionManager
                    |                         |
          "Does the route exist?"    "Send the exchange"
```

| SPI | Engine call site | What the Camel worker does |
|---|---|---|
| `ReactiveWorkerProvisioner` | `CaseContextChangedEventHandler.tryProvision()` | Capability probe — validates the route exists via `CamelCapabilityResolver`, returns `ProvisionResult.empty()` |
| `WorkerExecutionManager` | `WorkerScheduleEventHandler` | Actual dispatch — resolves the route URI, sends the exchange, manages sync/async completion |

Both are `@ApplicationScoped`. CDI displaces `NoOpReactiveWorkerProvisioner` and `NoOpWorkerExecutionManager` when the Camel worker jar is on the classpath.

### Sync vs async dispatch

The execution manager supports two exchange patterns, determined by the route's registration:

<!--
  Diagram: Sync dispatch (InOut)
  
  Engine ──submit()──> CamelWorkerExecutionManager
                              |
                     producerTemplate.request()
                              |
                         Camel Route
                              |
                     ┌────────┴────────┐
                     │                 │
                  success           exception/
                     │             FAULTED header
                     │                 │
          completionPublisher    faultPublisher
                     │                 │
        WORKER_EXECUTION_    CAMEL_WORKER_FAULT
           FINISHED               (retry logic)
-->

**InOut (synchronous):** The engine blocks (on a worker pool thread) until the route returns. On success, `WorkflowCompletionPublisher` fires `WORKER_EXECUTION_FINISHED`. On failure, `CamelWorkerFaultPublisher` fires `CAMEL_WORKER_FAULT`, entering the retry cycle.

<!--
  Diagram: Async dispatch (InOnly)
  
  Engine ──submit()──> CamelWorkerExecutionManager
                              |
                     register PendingCompletion
                              |
                     producerTemplate.send()  (fire-and-forget)
                              |
                         Camel Route ─── ... external work ...
                              |
                     ┌────────┴─────────────────┐
                     │                          │
              casehub:complete          REST callback
              (in-route)            POST /workers/complete/{id}
                     │                          │
              resolve pending            resolve pending
                     │                          │
            completionPublisher       completionPublisher
                 or faultPublisher       or faultCallbackEvent
-->

**InOnly (asynchronous):** The exchange is fire-and-forget. A `PendingCompletion` is registered in `AsyncWorkerCompletionRegistry` with a TTL (default: 60 minutes, configurable via `casehub.workers.async.timeout-minutes`). Completion arrives via one of two paths:

1. **In-route completion** — the route ends with `to("casehub:complete")`, which resolves the pending dispatch from within the Camel exchange.
2. **External REST callback** — an external system calls `POST /workers/complete/{dispatchId}` with header `X-Casehub-Callback-Token` for authentication.

If neither path fires within the TTL, `AsyncWorkerCompletionRegistry.expireStale()` (scheduled every 5 minutes by default) fires a `CompletionExpiredEvent`, which the Camel observer routes to the fault pipeline.

### Fault handling and retry

All faults — sync failures, async timeouts, explicit fault callbacks — funnel through a single path:

```
Any fault source
       |
WorkerFaultPublisher (Consumer<WorkerFaultEvent>)
       |
  WorkerFaultHandler (constructor-injected)
       |
  1. Persist WORKER_EXECUTION_FAILED EventLog
  2. Count prior failures for this (caseId, workerId, inputDataHash)
  3. failureCount < retryPolicy.maxAttempts()?
       |                    |
      yes                  no
       |                    |
  Thread.sleep         WORKER_RETRIES_EXHAUSTED
  + resubmit              (case stalls — human intervention)
  + re-submit()
```

**Retry policy:** Configured per-worker via `ExecutionPolicy.retries()`. If null, defaults to `new RetryPolicy()` — 3 attempts, 10-second FIXED backoff. The comparison is strict `<` (not `<=`): `failureCount < maxAttempts`.

**Backoff strategies:**

| Strategy | Behaviour | Cap |
|---|---|---|
| `FIXED` | Same delay every attempt | None |
| `EXPONENTIAL` | `baseDelay * 2^(attempt-1)` | 30 seconds |
| `EXPONENTIAL_WITH_JITTER` | Random value in `[0, exponential cap]` | 30 seconds |

**Why CAMEL_WORKER_FAULT, not WORKFLOW_EXECUTION_FAILED:** The Quartz scheduler module listens on `WORKFLOW_EXECUTION_FAILED`. If Camel faults were published there, Quartz would double-process them. Camel has its own fault address to prevent this.

**Threading:** The Vert.x timer fires on the event loop. `emitOn(Infrastructure.getDefaultWorkerPool())` re-dispatches to the worker pool before calling `submit()`, which performs blocking I/O. This avoids blocking the event loop.

## Capability resolution

`CamelCapabilityResolver` maps capability tags (strings from the case definition) to Camel route URIs. Resolution happens at startup, in priority order:

| Tier | Source | How to register | Exchange pattern |
|---|---|---|---|
| 1 | `CamelWorkerRoute` SPI beans | Implement `CamelWorkerRoute`, make it a CDI bean | Set explicitly via `exchangePattern()` |
| 2 | Configuration properties | `casehub.workers.camel.capabilities.<tag>=<uri>` | InOnly (async) |
| 3 | Convention | Route with `routeId` = capability tag AND `from("direct:<tag>")` | InOnly (async) |

Tier 1 wins over Tier 2 wins over Tier 3 (`putIfAbsent` semantics). All three tiers are checked; results are merged.

### Registering a route — Tier 1 (SPI bean)

```java
@ApplicationScoped
public class SendEmailRoute extends RouteBuilder implements CamelWorkerRoute {

    @Override
    public String capabilityTag() { return "send-email"; }

    @Override
    public String entryUri() { return "direct:send-email"; }

    @Override
    public ExchangePattern exchangePattern() { return ExchangePattern.InOut; }

    @Override
    public void configure() {
        from("direct:send-email")
            .to("smtp://mail.example.com");
    }
}
```

### Registering a route — Tier 2 (configuration)

```properties
casehub.workers.camel.capabilities.send-email=direct:send-email
```

### Registering a route — Tier 3 (convention)

If a route's `routeId` matches a `direct:<routeId>` `from()` endpoint, it auto-registers. No explicit registration needed:

```java
public class SendEmailRoute extends RouteBuilder {
    @Override
    public void configure() {
        from("direct:send-email").routeId("send-email")
            .to("smtp://mail.example.com");
    }
}
```

## Exchange headers

Headers set on the exchange before it reaches the route:

| Header | Type | Present in | Value |
|---|---|---|---|
| `casehub-idempotency` | `String` | Sync + Async | Hash of `(caseId, workerName, capabilityTag, inputData)` |
| `casehub-case-id` | `String` | Sync + Async | Case instance UUID |
| `casehub-tenancy-id` | `String` | Sync + Async | Tenant identifier |
| `casehub-task-type` | `String` | Sync + Async | Capability tag |
| `casehub-worker-id` | `String` | Async only | Dispatch ID for completion correlation |
| `casehub-callback-token` | `String` | Async only | Token for REST callback authentication |

### Signalling fault from within a route

Set the header `casehub-work-status` to `FAULTED` on the exchange. The execution manager checks for this alongside `exchange.getException()`:

```java
from("direct:my-route")
    .process(exchange -> {
        // ... something went wrong but not an exception ...
        exchange.getIn().setHeader("casehub-work-status", "FAULTED");
    });
```

## The `casehub:complete` component

A Camel component for completing async dispatches from within a route, without an external REST callback. Producer-only — consuming from `casehub:complete` throws `UnsupportedOperationException`.

```java
from("direct:long-running-job")
    .to("http://batch-system/submit")
    .delay(30000)
    .to("http://batch-system/poll-result")
    .to("casehub:complete");
```

The component reads `casehub-worker-id` from the exchange headers, resolves the `PendingCompletion` from the registry, and fires completion or fault depending on exchange state. If the dispatch ID is not found (already resolved or expired), it logs a warning and returns — no exception.

## REST callback endpoint

For external systems that can't use the `casehub:complete` component (because they run outside the Camel context), `workers-common` provides:

```
POST /workers/complete/{dispatchId}
Content-Type: application/json
X-Casehub-Callback-Token: <token from casehub-callback-token header>
```

Request body:

```json
{
  "output": { "key": "value" },
  "faulted": false,
  "errorMessage": null
}
```

| Field | Type | Required | Purpose |
|---|---|---|---|
| `output` | `Map<String, Object>` | No | Output data passed to completion event |
| `faulted` | `boolean` | Yes | `true` routes to fault pipeline instead of completion |
| `errorMessage` | `String` | No | Error message when `faulted: true` |

**Authentication:** The callback token is compared using `MessageDigest.isEqual()` (constant-time) to prevent timing attacks. On token mismatch, the pending completion is re-registered with remaining TTL — the dispatch is not lost, just this callback attempt is rejected.

**Response codes:**

| Code | Meaning |
|---|---|
| 200 | Completion (or fault) processed |
| 401 | Callback token mismatch — pending re-registered |
| 404 | Dispatch ID not found (expired or already resolved) |

## Configuration

| Property | Default | Purpose |
|---|---|---|
| `casehub.workers.async.timeout-minutes` | `60` | TTL for async pending completions |
| `casehub.workers.async.expiry-check-interval` | `5m` | How often `expireStale()` runs |
| `casehub.workers.camel.capabilities.<tag>` | — | Tier 2 capability registration: maps tag to route URI |

## Module structure

```
workers-camel/
  src/main/java/io/casehub/workers/camel/
    CamelWorkerConstants.java            WORKER_TYPE = "camel"
    CamelWorkerRoute.java               SPI interface for Tier 1 registration
    CamelCapabilityResolver.java         3-tier tag → URI resolution
    CamelReactiveWorkerProvisioner.java  Capability probe (does the route exist?)
    CamelWorkerExecutionManager.java     Dispatch: sync (InOut) or async (InOnly)
    CamelWorkerFaultPublisher.java       Publishes via Consumer<WorkerFaultEvent>
    CamelCompletionExpiryObserver.java   Routes async timeout → fault pipeline
    CamelFaultCallbackObserver.java      Routes REST fault callback → fault pipeline
    component/
      CasehubComponent.java             Camel component: casehub:complete
      CasehubEndpoint.java              Producer-only endpoint
      CasehubProducer.java              Resolves pending, fires completion/fault
```

### Dependencies on `workers-common`

The Camel worker does not implement completion tracking, callback endpoints, or correlation context — those live in `workers-common` and are shared across all worker types:

| `workers-common` type | Used by Camel worker for |
|---|---|
| `AsyncWorkerCompletionRegistry` | Register/resolve async pending completions |
| `PendingCompletion` | Per-dispatch tracking record |
| `WorkerCorrelationContext` | Case instance + worker + idempotency + tenancyId |
| `WorkflowCompletionPublisher` | Fire `WORKER_EXECUTION_FINISHED` on success |
| `WorkerCallbackResource` | REST callback endpoint (no Camel-specific code) |
| `CasehubWorkerHeaders` | Header name constants |
| `CompletionExpiredEvent` | CDI async event for TTL expiry |
| `FaultCallbackEvent` | CDI async event for faulted REST callbacks |
| `WorkerCapabilityResolver<T>` | Interface for capability tag resolution |

### CDI event observers filter by `workerType`

When multiple worker modules are co-deployed, CDI async events (`CompletionExpiredEvent`, `FaultCallbackEvent`) reach all observers. Each observer checks `pending.workerType()` and returns immediately if it doesn't match:

```java
void onExpiry(@ObservesAsync CompletionExpiredEvent event) {
    if (!CamelWorkerConstants.WORKER_TYPE.equals(event.pending().workerType())) return;
    // ... handle ...
}
```

This is not optional — without the filter, a Camel expiry observer would process HTTP worker timeouts and vice versa.

## Co-deployment constraints

| Combination | Status | Issue |
|---|---|---|
| `workers-camel` alone | Supported | — |
| `workers-camel` + `scheduler-quartz` | **Not supported** | CDI ambiguity on `WorkerExecutionManager` — both provide an `@ApplicationScoped` implementation. Requires a composite manager in the engine. |
| `workers-camel` + `workers-http` | **Not supported** | Same `WorkerExecutionManager` CDI ambiguity. The `workerType` discriminator in `PendingCompletion` prevents CDI event cross-talk, but the SPI conflict remains. |

## Building

```bash
# Build all worker modules (from repo root)
mvn --batch-mode install

# Build only the Camel worker
mvn --batch-mode install -pl workers-camel -am
```

Requires Java 21. Dependencies (`casehub-engine-api`, `casehub-engine-common`, `casehub-platform-api`) are resolved from GitHub Packages — configure `~/.m2/settings.xml` with a `GITHUB_TOKEN` that has `read:packages` scope.
