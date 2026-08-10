# CaseHub HTTP Worker — Design Spec

**Status:** Approved (rev 3)
**Date:** 2026-06-09
**Issue:** casehubio/casehub-workers#5
**Branch:** `issue-5-design-implement-workers-http`
**Review:** 3 cycles — 24 issues total, 21 accepted, 1 self-corrected, 2 confirmed correct

---

## 1. Purpose

An HTTP worker that dispatches case steps to external services via HTTP. Any REST endpoint becomes a dispatchable unit of work within a case lifecycle — the case engine handles orchestration and retry; the external service handles the business logic.

This is the zero-friction adoption path for CaseHub workers. Existing services receive an HTTP request without modification. No SDK, no agent, no Camel route — just an HTTP endpoint.

## 2. When to use this (and when not to)

| Situation | Right approach |
|---|---|
| Existing service that a case needs to call | **HTTP worker** |
| New integration built for a case flow | Camel worker (tighter lifecycle integration) |
| Standalone service, no case context | Don't use a worker at all |
| Complex response interpretation (body parsing, status code remapping) | Camel worker or quarkus-flow `call: http` |

The HTTP worker's value is simplicity. If you need to interpret response bodies, remap status codes, or do anything beyond "send request, get response," use a Camel route.

## 3. Architecture

### 3.1 Engine SPI implementations

Two `@ApplicationScoped` beans, same pattern as the Camel worker. CDI displaces `NoOpReactiveWorkerProvisioner` and `NoOpWorkerExecutionManager` when the HTTP worker jar is on the classpath.

| SPI | Implementation | Purpose |
|---|---|---|
| `ReactiveWorkerProvisioner` | `HttpReactiveWorkerProvisioner` | Capability probe — validates the endpoint is registered (not reachable — no health-check at provision time) |
| `WorkerExecutionManager` | `HttpWorkerExecutionManager` | Dispatch — sends the HTTP request, manages sync/async completion |

**`ReactiveWorkerProvisioner` methods:**
- `provision(capabilities, context)` — calls `httpEndpointResolver.firstMatch(capabilities)` then `resolve(capability)` to verify the endpoint is registered. Returns `ProvisionResult.empty()`. Throws `WorkerProvisioningException` if no endpoint resolves.
- `terminate(workerId)` — returns `Uni.createFrom().voidItem()` (HTTP endpoints are stateless)
- `getCapabilities()` — returns `httpEndpointResolver.capabilities()`

**`WorkerExecutionManager` methods:**
- `submit(eventLogId, instance, worker, capability, inputData)` — Section 5 covers this
- `schedulePersistedEvent(scheduledEventLog)` — returns `Uni.createFrom().voidItem()` (no Quartz persisted events)
- `getActiveWorkCount(workerId)` — delegates to `asyncWorkerCompletionRegistry.countByWorkerName(workerId)`. **Note:** tracks async dispatches only. Sync HTTP calls are in-flight for at most `timeoutSeconds` and are not tracked in the registry. This matches the Camel worker's behaviour, but is more significant here because the HTTP worker's primary mode is sync.
- `getActiveCaseIds(workerId)` — inherits default `List.of()`

### 3.2 Module structure

```
workers-http/
  src/main/java/io/casehub/workers/http/
    HttpWorkerConstants.java              WORKER_TYPE = "http"
    HttpWorkerEventBusAddresses.java      HTTP_WORKER_FAULT address
    HttpWorkerRoute.java                  SPI interface for Tier 1 registration
    HttpEndpointResolver.java             3-tier tag → endpoint resolution
    HttpReactiveWorkerProvisioner.java     Capability probe
    HttpWorkerExecutionManager.java        Dispatch via Vert.x WebClient
    HttpWorkerFaultPublisher.java          Publishes to HTTP_WORKER_FAULT
    HttpWorkerFaultEventHandler.java       Retry logic + Retry-After + PermanentFaultException
    HttpCompletionExpiryObserver.java      Routes async timeout → fault pipeline
    HttpFaultCallbackObserver.java         Routes REST fault callback → fault pipeline
    ExchangeMode.java                     Enum: SYNC, ASYNC
    ResolvedEndpoint.java                 Resolution result record
    PermanentFaultException.java          Marks 4xx faults as non-retryable
    RetryAfterException.java             Carries retryAfterMs from 429 Retry-After header
```

### 3.3 Dependencies

| Dependency | Purpose |
|---|---|
| `casehub-workers-common` | Completion registry, callback endpoint, fault events, headers, `WorkerRetrySupport` |
| `casehub-engine-api` | `ReactiveWorkerProvisioner`, `Worker`, `Capability`, `ExecutionPolicy`, `RetryPolicy` |
| `casehub-engine-common` | `WorkerExecutionManager`, `WorkflowExecutionCompleted`, `WorkflowExecutionFailed`, `CaseInstance`, `EventLog`, `EventBusAddresses`, `EventLogRepository`, `WorkerExecutionKeys` |
| `casehub-platform-api` | Tenant propagation |
| `quarkus-arc` | CDI |
| `quarkus-vertx` | Vert.x core — `EventBus`, `Vertx.setTimer()` for retry delays |
| `smallrye-mutiny-vertx-web-client` | Vert.x Mutiny `WebClient` for reactive HTTP dispatch |

Test dependencies: `casehub-workers-testing`, `quarkus-junit`, `assertj-core`, `mockito-core`.

**POM changes from skeleton:** The existing `workers-http/pom.xml` needs updating:
- **Add:** `casehub-workers-common`, `casehub-engine-common`, `quarkus-vertx`, `smallrye-mutiny-vertx-web-client`, `mockito-core` (test)
- **Remove:** `quarkus-rest-client-jackson` (not needed with Vert.x WebClient)

## 4. Endpoint Resolution

`HttpEndpointResolver` implements `WorkerCapabilityResolver<ResolvedEndpoint>` — the same interface that `CamelCapabilityResolver` implements as `WorkerCapabilityResolver<String>`. The generic parameter is the resolution result type: Camel resolves to a URI string, HTTP resolves to a `ResolvedEndpoint` record.

Initialised at startup via `@Observes @Priority(APPLICATION) StartupEvent`. Same `putIfAbsent` priority semantics as Camel's resolver.

### 4.1 Three-tier resolution

| Tier | Source | Registration | Exchange mode |
|---|---|---|---|
| 1 | `HttpWorkerRoute` SPI beans | Implement interface, make CDI bean | Set explicitly via `exchangeMode()` |
| 2 | Configuration properties | `casehub.workers.http.endpoints.<tag>.*` | Config property, default `sync` |
| 3 | `EndpointRegistry` SPI (platform#73) | Automatic — queries registry for unresolved tags | Default `sync` |

Tier 1 wins over Tier 2 wins over Tier 3. All three tiers are checked; results are merged.

### 4.2 Tier 1 — `HttpWorkerRoute` SPI interface

```java
public interface HttpWorkerRoute {
    String capabilityTag();
    String url();

    /** HTTP method. Default POST. */
    default String method() { return "POST"; }

    /** Sync (wait for response) or async (fire-and-forget + callback). Default SYNC. */
    default ExchangeMode exchangeMode() { return ExchangeMode.SYNC; }

    /** Static headers added to every request (e.g. Authorization). Default empty. */
    default Map<String, String> headers() { return Map.of(); }

    /**
     * Timeout in seconds for sync HTTP calls.
     * Returns -1 to inherit from casehub.workers.http.default-timeout-seconds.
     * Ignored for async mode (async uses the completion registry TTL instead).
     */
    default int timeoutSeconds() { return -1; }
}
```

Example:

```java
@ApplicationScoped
public class InvoiceEndpoint implements HttpWorkerRoute {
    @Override public String capabilityTag() { return "send-invoice"; }
    @Override public String url() { return "https://billing.example.com/invoices"; }
    @Override public Map<String, String> headers() {
        return Map.of("Authorization", "Bearer " + apiKey);
    }
}
```

### 4.3 Tier 2 — Configuration properties

```properties
casehub.workers.http.endpoints.send-invoice.url=https://billing.example.com/invoices
casehub.workers.http.endpoints.send-invoice.method=POST
casehub.workers.http.endpoints.send-invoice.mode=sync
casehub.workers.http.endpoints.send-invoice.timeout-seconds=30
casehub.workers.http.endpoints.send-invoice.headers.Authorization=Bearer sk-xxx
casehub.workers.http.endpoints.send-invoice.headers.X-Custom=value
```

Defaults: `method=POST`, `mode=sync`, `timeout-seconds` inherits from `casehub.workers.http.default-timeout-seconds`.

### 4.4 Tier 3 — EndpointRegistry (platform#73)

When `EndpointRegistry` is on the classpath, the resolver queries it for any capability tag not resolved by Tier 1 or 2. The `EndpointDescriptor` provides URL and `credentialRef`; auth is resolved from the registry's credential mechanism.

Until platform#73 ships, Tier 3 is a no-op. The resolver checks for an injected `Instance<EndpointRegistry>` and skips if unsatisfied. The HTTP worker is designed to work without it.

### 4.5 Resolved endpoint record

```java
record ResolvedEndpoint(
    String url,
    String method,
    ExchangeMode mode,
    Map<String, String> headers,
    int timeoutSeconds
) {}
```

### 4.6 URI template interpolation

URLs may contain `{fieldName}` placeholders that are interpolated from top-level `inputData` keys at dispatch time. This is resolved in `HttpWorkerExecutionManager.submit()`, not in the resolver — the resolver stores the URL as-is (it may be a template).

```properties
# Config example with path parameter
casehub.workers.http.endpoints.ship-order.url=https://api.example.com/orders/{orderId}/ship
```

At dispatch time with `inputData = {"orderId": "ORD-123", "priority": "express"}`, the URL becomes `https://api.example.com/orders/ORD-123/ship`.

**Rules:**
- Only top-level `inputData` keys are available — nested paths (`{order.id}`) are not supported
- Values are converted via `toString()` and URL-encoded
- A `{fieldName}` with no matching key in `inputData` is a permanent fault (`PermanentFaultException`) — the `inputData` is replayed from `EventLog` on retry, so the same key will be missing every time. Retrying is pointless.
- URLs without `{...}` tokens are passed through unchanged (zero-cost no-op)

## 5. Dispatch

`HttpWorkerExecutionManager.submit()` resolves the endpoint via `HttpEndpointResolver`, interpolates any URI template placeholders from `inputData`, then branches on `ExchangeMode`.

### 5.1 Sync dispatch

```
submit() → resolve endpoint
         → interpolate URL template from inputData
         → webClient.requestAbs(method, url)
              .timeout(timeoutSeconds * 1000)
              .putHeaders(casehub headers + endpoint headers)
              .sendJson(inputData)
         → on 2xx: completionPublisher.complete(ctx, responseBody as Map)
         → on 429: faultPublisher.fault(...) with RetryAfterException(retryAfterMs)
         → on 5xx: faultPublisher.fault(...)
         → on 4xx (not 429): faultPublisher.fault(...) with PermanentFaultException
         → on connection failure/timeout: faultPublisher.fault(...)
```

Fully reactive — Vert.x `WebClient` returns `Uni<HttpResponse<Buffer>>` on the event loop. No `runSubscriptionOn(workerPool)` needed. This is a material improvement over the Camel worker's threading model where `ProducerTemplate.request()` is blocking.

### 5.2 Async dispatch

```
submit() → resolve endpoint
         → interpolate URL template from inputData
         → register PendingCompletion in AsyncWorkerCompletionRegistry
              (workerType = "http", TTL = casehub.workers.async.timeout-minutes)
         → webClient.requestAbs(method, url)
              .putHeaders(casehub headers + endpoint headers + dispatch ID + callback token)
              .sendJson(inputData)
         → on 2xx: return (fire-and-forget — completion via callback)
         → on non-2xx / connection failure: faultPublisher.fault(...)
```

Async dispatch adds `casehub-worker-id` and `casehub-callback-token` to the request headers. The external service calls back to `POST /workers/complete/{dispatchId}` (provided by `workers-common`) when done.

### 5.3 Request headers

Headers set on every outbound request:

| Header | Type | Present in | Value |
|---|---|---|---|
| `casehub-idempotency` | `String` | Sync + Async | Composite key: `caseId:workerName:capabilityTag:sha256(inputData)` via `WorkerExecutionKeys.inputDataHash()` |
| `casehub-case-id` | `String` | Sync + Async | Case instance UUID |
| `casehub-tenancy-id` | `String` | Sync + Async | Tenant identifier |
| `casehub-task-type` | `String` | Sync + Async | Capability tag |
| `casehub-worker-id` | `String` | Async only | Dispatch ID for completion correlation |
| `casehub-callback-token` | `String` | Async only | Token for REST callback authentication |

Endpoint-configured headers (Authorization, custom headers) are added after CaseHub headers. If an endpoint header collides with a CaseHub header, the endpoint header wins — allows overriding defaults when necessary.

### 5.4 Response handling

**Sync success (2xx):** The response body is deserialized as `Map<String, Object>` via Jackson and passed to `completionPublisher.complete()`. If the response body is empty or not valid JSON, an empty `Map.of()` is used.

No output mapping is performed at the worker level. The engine's case-definition-level `outputMapping` handles any reshaping.

## 6. Fault Handling and Retry

### 6.1 Error classification

Fixed — not configurable per endpoint.

| Status | Classification | Behaviour |
|---|---|---|
| 2xx | Success | Complete with response body |
| 429 | Transient fault | Retry; honour `Retry-After` header if present |
| 5xx | Transient fault | Retry via fault pipeline |
| 4xx (except 429) | Permanent fault | Fault immediately — `PermanentFaultException` bypasses retry |
| Connection timeout / refused | Transient fault | Retry via fault pipeline |

If you need to treat specific status codes differently (e.g., 404 as a business outcome), use a Camel worker or quarkus-flow `call: http` step.

### 6.2 Fault pipeline

All faults funnel through a single path, identical to Camel's structure:

```
Any fault source
       │
HttpWorkerFaultPublisher
       │
  eventBus.publish(HTTP_WORKER_FAULT)
       │
HttpWorkerFaultEventHandler (@ConsumeEvent, blocking)
       │
  1. retrySupport.persistFailureLog(...) (always — for observability)
  2. Check PermanentFaultException → retrySupport.publishRetriesExhausted(...)
  3. retrySupport.countFailedAttempts(...)
  4. Check retry eligibility
       │
  ┌────┴────────────────────────┐
  │                             │
PermanentFaultException?       Normal fault
  │                             │
  │                    failureCount < maxAttempts?
  │                        │              │
  │                       yes             no
  │                        │              │
  ▼                   Vert.x timer    retrySupport
WORKER_RETRIES_      + re-submit()    .publishRetriesExhausted(...)
  EXHAUSTED
```

The `WORKER_EXECUTION_FAILED` EventLog is always written regardless of whether the fault is permanent or transient. This ensures the audit trail is complete for observability. The `PermanentFaultException` check happens after persistence but before the retry eligibility check — permanent faults are recorded but never retried.

### 6.3 `WorkerRetrySupport` — shared retry infrastructure (workers-common)

The retry building blocks are extracted to `workers-common` as a shared `@ApplicationScoped` bean. Both the Camel fault handler and the HTTP fault handler inject it. The Camel handler is refactored as part of this implementation.

```java
@ApplicationScoped
public class WorkerRetrySupport {
    @Inject EventLogRepository eventLogRepository;
    @Inject EventBus eventBus;

    public Uni<Void> persistFailureLog(CaseInstance instance, Worker worker,
                                        String inputDataHash, String errorMsg,
                                        String tenancyId) { ... }

    public Uni<Long> countFailedAttempts(UUID caseId, String workerId,
                                          String inputDataHash, String tenancyId) { ... }

    public void publishRetriesExhausted(UUID caseId, String workerId,
                                         String inputDataHash) { ... }

    public static RetryPolicy resolveRetryPolicy(Worker worker) { ... }

    public static long computeBackoffDelayMs(RetryPolicy policy, long attemptNumber) { ... }
}
```

**What stays in each worker's fault handler:**
- Fault classification (`PermanentFaultException`, `RetryAfterException` — HTTP only)
- Threading on retry (`emitOn(workerPool)` for Camel, bare event loop for HTTP)
- The fault event bus address constant
- The Vert.x timer + re-submit orchestration

**Camel refactoring scope:** `CamelWorkerFaultEventHandler` replaces its private `resolveRetryPolicy()`, `computeBackoffDelayMs()`, `countFailedAttempts()`, failure EventLog construction, and exhaustion publishing with calls to `WorkerRetrySupport`. The private `reloadAndResubmit()` stays — it owns the Camel-specific `emitOn(workerPool)` orchestration. This is a mechanical extraction, not a design change.

### 6.4 HTTP-specific enhancements over Camel

**`PermanentFaultException`:** Thrown on 4xx responses (except 429). `HttpWorkerFaultEventHandler` checks `if (cause instanceof PermanentFaultException)` after calling `retrySupport.persistFailureLog()` and before counting prior failures. Calls `retrySupport.publishRetriesExhausted()` immediately — no count query, no retry. Retrying a 400 Bad Request is pointless.

**`RetryAfterException`:** Thrown on 429 responses. Carries `retryAfterMs` extracted from the `Retry-After` header. `HttpWorkerFaultEventHandler` checks `if (cause instanceof RetryAfterException ra)` and uses `ra.retryAfterMs()` as the Vert.x timer delay instead of the configured backoff strategy.

**`Retry-After` parsing rules:**
- Seconds format (e.g., `Retry-After: 30`) — parsed as `30 * 1000` ms
- HTTP-date format (e.g., `Retry-After: Mon, 09 Jun 2026 14:30:00 GMT`) — computed as `dateMs - nowMs`
- HTTP-date in the past — treated as 0ms (immediate retry)
- Invalid or unparseable value — no `RetryAfterException` thrown; falls back to configured backoff (plain `RuntimeException` instead)
- No `Retry-After` header on 429 — no `RetryAfterException`; falls back to configured backoff

**Retry-After TTL capping (async only):** For async dispatches, if `retryAfterMs` exceeds the remaining TTL on the `PendingCompletion`, it is capped to prevent the pending completion from expiring before the retry fires. Sync dispatches use the `Retry-After` value directly — there is no `PendingCompletion` with a TTL to cap against. The retry fires a new sync HTTP call with its own timeout. No maximum cap is applied to sync `Retry-After` values.

### 6.5 Retry policy

Identical to Camel (now shared via `WorkerRetrySupport`). Configured per-worker via `ExecutionPolicy.retries()`. Null defaults to `new RetryPolicy()` — 3 attempts, 10s FIXED backoff. Comparison is strict `<`: `failureCount < maxAttempts`.

Backoff strategies:

| Strategy | Behaviour | Cap |
|---|---|---|
| `FIXED` | Same delay every attempt | None |
| `EXPONENTIAL` | `baseDelay * 2^(attempt-1)` | 30 seconds |
| `EXPONENTIAL_WITH_JITTER` | Random value in `[0, exponential cap]` | 30 seconds |

### 6.6 Fault address separation

`HTTP_WORKER_FAULT = "casehub.workers.http.fault"` — separate from both `CAMEL_WORKER_FAULT` and `WORKFLOW_EXECUTION_FAILED`. Same reasoning: the Quartz scheduler listens on `WORKFLOW_EXECUTION_FAILED`, and each worker type's fault handler owns its own retry logic.

### 6.7 Threading

Different from Camel. The Vert.x timer fires on the event loop. The Camel worker uses `emitOn(Infrastructure.getDefaultWorkerPool())` after the timer because `ProducerTemplate.request()` is blocking I/O. The HTTP worker does **not** need `emitOn` — `WebClient` is event-loop native and non-blocking. Adding `emitOn` would waste a context switch for no benefit.

The retry path: `Vert.x timer → emitter completes on event loop → submit() called on event loop → WebClient sends non-blocking request → response arrives on event loop → completion/fault published on event loop`. All non-blocking, all event-loop safe.

The fault handler injects `io.vertx.core.Vertx` (bare Vert.x, not the Mutiny wrapper) for `vertx.setTimer()`, and `io.vertx.mutiny.core.eventbus.EventBus` for `WORKER_RETRIES_EXHAUSTED` publishing.

### 6.8 CDI event observers

Both observers filter by `workerType`, required for co-deployment with other worker modules:

```java
void onExpiry(@ObservesAsync CompletionExpiredEvent event) {
    if (!HttpWorkerConstants.WORKER_TYPE.equals(event.pending().workerType())) return;
    faultPublisher.fault(event.pending(), new RuntimeException("Async timeout"));
}
```

## 7. Authentication

Follows the **Outbound Authentication** coherence policy in PLATFORM.md. The canonical model is Serverless Workflow 1.0's `AuthenticationPolicy` vocabulary.

For the initial implementation, auth is configured as static headers on the endpoint (Tier 1 headers or Tier 2 config properties). This covers Bearer tokens, Basic auth, and API keys — the most common patterns.

OAuth2 client credentials (with token lifecycle) and `EndpointRegistry` integration (with `credentialRef` secret resolution) are deferred to platform#73 and the secrets backend. The HTTP worker is designed to activate these by classpath presence when they ship — no code changes required.

## 8. Configuration

| Property | Default | Purpose |
|---|---|---|
| `casehub.workers.http.default-timeout-seconds` | `30` | Global default timeout for sync HTTP calls |
| `casehub.workers.http.endpoints.<tag>.url` | — | Tier 2: endpoint URL (required, may contain `{fieldName}` templates) |
| `casehub.workers.http.endpoints.<tag>.method` | `POST` | HTTP method |
| `casehub.workers.http.endpoints.<tag>.mode` | `sync` | `sync` or `async` |
| `casehub.workers.http.endpoints.<tag>.timeout-seconds` | inherits global | Per-endpoint timeout override |
| `casehub.workers.http.endpoints.<tag>.headers.<name>` | — | Static headers (including auth) |
| `casehub.workers.async.timeout-minutes` | `60` | TTL for async pending completions (from workers-common) |
| `casehub.workers.async.expiry-check-interval` | `5m` | Expiry scan interval (from workers-common) |

## 9. Co-deployment Constraints

| Combination | Status | Issue |
|---|---|---|
| `workers-http` alone | Supported | — |
| `workers-http` + `workers-camel` | Not supported | CDI ambiguity on `WorkerExecutionManager`. `workerType` discriminator prevents CDI event cross-talk, but the SPI conflict remains. Requires a composite manager in the engine (engine#461). |
| `workers-http` + `scheduler-quartz` | Not supported | Same `WorkerExecutionManager` CDI ambiguity |

## 10. Dependencies on workers-common

The HTTP worker reuses all shared infrastructure from `workers-common`:

| Type | Used for |
|---|---|
| `AsyncWorkerCompletionRegistry` | Register/resolve async pending completions |
| `PendingCompletion` | Per-dispatch tracking record |
| `WorkerCorrelationContext` | Case instance + worker + idempotency + tenancyId |
| `WorkflowCompletionPublisher` | Fire `WORKER_EXECUTION_FINISHED` on success |
| `WorkerCallbackResource` | REST callback endpoint (no HTTP-specific code) |
| `CasehubWorkerHeaders` | Header name constants |
| `CompletionExpiredEvent` | CDI async event for TTL expiry |
| `FaultCallbackEvent` | CDI async event for faulted REST callbacks |
| `WorkerCapabilityResolver<T>` | Interface for capability tag resolution |
| `WorkerRetrySupport` | **New** — shared retry building blocks (persist failure, count attempts, resolve policy, compute backoff, publish exhaustion) |
| `WorkerStatusPublisher` | **Not used** — wraps `ReactiveWorkerStatusListener` (onWorkerStarted/Completed/Stalled). Neither the Camel worker nor the HTTP worker calls these hooks. Deferred until the engine defines when worker status notifications are expected. |

## 11. Implementation Notes

**engine#447 (stale reference):** `NoOpWorkerExecutionManager @DefaultBean` now exists in the engine (issue CLOSED). CLAUDE.md's Cross-Repo Dependencies section still references it as pending — clean up during implementation.

## 12. Test Coverage

### `WorkerRetrySupport` (new, in workers-common)

- `persistFailureLog()` writes EventLog with `WORKER_EXECUTION_FAILED` type, correct metadata (`inputDataHash`, `errorMessage`)
- `countFailedAttempts()` counts only entries matching `inputDataHash` in metadata — entries with different hash not counted
- `publishRetriesExhausted()` publishes `WorkerRetriesExhaustedEvent` on `WORKER_RETRIES_EXHAUSTED` address
- `resolveRetryPolicy()` with null `ExecutionPolicy` → default `RetryPolicy(3, 10000, FIXED)`
- `resolveRetryPolicy()` with null `retries()` on policy → default `RetryPolicy(3, 10000, FIXED)`
- `resolveRetryPolicy()` with explicit policy → returns it unchanged
- `computeBackoffDelayMs()` — FIXED returns baseDelay, EXPONENTIAL caps at 30s, EXPONENTIAL_WITH_JITTER in `[0, cap]`

### `HttpEndpointResolver`

- Tier 1 SPI bean wins over Tier 2 config for same capability tag
- Tier 2 config wins over Tier 3 EndpointRegistry for same capability tag
- All three tiers merged — tags from different tiers all resolve
- `resolve()` for unknown tag → `WorkerProvisioningException`
- `firstMatch()` returns first matching tag from input set
- `capabilities()` returns all resolved tags from all tiers
- Config defaults: method=POST, mode=SYNC when not specified
- `timeoutSeconds = -1` on SPI bean → inherits global default

### `HttpWorkerExecutionManager` — sync

- 2xx → `completionPublisher.complete()` called with response body as `Map<String, Object>`
- Empty response body → completion with `Map.of()`
- Non-JSON response body → completion with `Map.of()`
- 4xx (not 429) → `PermanentFaultException` → `retrySupport.publishRetriesExhausted()` immediately, no retry
- 429 with `Retry-After: 30` → `RetryAfterException(30000)` → retry with 30s delay
- 429 with `Retry-After: <past HTTP-date>` → `RetryAfterException(0)` → immediate retry
- 429 with unparseable `Retry-After` → plain `RuntimeException` → retry with configured backoff
- 429 without `Retry-After` header → retry with configured backoff (not `RetryAfterException`)
- 5xx → fault via pipeline, retry with configured backoff
- Connection timeout → fault via pipeline, retry
- Connection refused → fault via pipeline, retry
- CaseHub headers set on request (idempotency, case-id, tenancy-id, task-type)
- Endpoint header overrides CaseHub header with same name
- URI template `{orderId}` interpolated from inputData `{"orderId": "123"}` → URL contains `/123/`
- URI template `{missing}` with no matching inputData key → `PermanentFaultException` (inputData replayed on retry — same key will be missing)
- URL without `{...}` tokens → passed through unchanged

### `HttpWorkerExecutionManager` — async

- 2xx → fire-and-forget, `PendingCompletion` registered with `workerType = "http"`
- Non-2xx → immediate fault, `PendingCompletion` remains (expiry will clean up)
- `casehub-worker-id` and `casehub-callback-token` headers set on async requests
- `casehub-worker-id` and `casehub-callback-token` NOT set on sync requests

### `HttpWorkerFaultEventHandler`

- `retrySupport.persistFailureLog()` called for every fault (permanent and transient)
- `PermanentFaultException` → `retrySupport.publishRetriesExhausted()` immediately, no count query
- `RetryAfterException` → timer uses `retryAfterMs`, not configured backoff
- Retry-After capped to remaining TTL for async dispatches
- Retry-After NOT capped for sync dispatches (no PendingCompletion)
- `failureCount < maxAttempts` (strict `<`) — verify 3rd failure with default policy (3 attempts) exhausts
- Null `ExecutionPolicy` → defaults to `RetryPolicy()` (3 attempts, 10s FIXED)
- Retry calls `submit()` directly on event loop (no `emitOn` — WebClient is non-blocking)

### `HttpCompletionExpiryObserver`

- `CompletionExpiredEvent` with `workerType = "http"` → fault pipeline fires
- `CompletionExpiredEvent` with `workerType = "camel"` → observer returns without firing (workerType filter)

### `HttpFaultCallbackObserver`

- `FaultCallbackEvent` with `workerType = "http"` → fault pipeline fires
- `FaultCallbackEvent` with `workerType = "camel"` → observer returns without firing

### `HttpReactiveWorkerProvisioner`

- `getCapabilities()` returns all resolved tags
- `provision()` with resolvable capability → `ProvisionResult.empty()`
- `provision()` with unresolvable capability → `WorkerProvisioningException`
- `terminate()` → `Uni.createFrom().voidItem()`

### Camel refactoring verification

- `CamelWorkerFaultEventHandler` uses `WorkerRetrySupport` for all shared operations
- Existing Camel fault handler tests still pass after refactoring
- No behaviour change — same retry semantics, same EventLog entries

## 13. What This Spec Does Not Cover

- **OAuth2 token lifecycle** — deferred to `EndpointRegistry` (platform#73) and secrets backend
- **Request body transformation** — always JSON serialization of `inputData`. JQ/template-based request shaping is out of scope; use Camel for that.
- **Response body interpretation** — always raw passthrough. Status code remapping and body parsing are Camel/quarkus-flow concerns.
- **Composite `WorkerExecutionManager`** — required for co-deploying HTTP + Camel + Quartz. Engine concern, tracked as engine#461.
- **`casehub:complete` equivalent** — the Camel worker has a Camel component for in-route completion. The HTTP worker doesn't need one — external services use the REST callback.
- **`WorkerStatusPublisher` integration** — neither Camel nor HTTP worker currently calls `onWorkerStarted`/`onWorkerCompleted`/`onWorkerStalled`. Deferred until the engine defines the contract for when these notifications are expected.
