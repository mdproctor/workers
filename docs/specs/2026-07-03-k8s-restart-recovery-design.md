# K8s Restart Recovery Design

**Issue:** casehubio/workers#17
**Status:** Approved
**Date:** 2026-07-03

## Problem

When the worker process restarts, the in-memory `AsyncWorkerCompletionRegistry` is lost. For K8s Jobs — which can run for hours — this causes two failures:

1. **Unobserved completions (Scenario 1).** Jobs that complete after restart have no `PendingCompletion` in the registry. `processTerminal()` calls `registry.complete(dispatchId)` → empty → silently drops the result. Cases remain stuck in a waiting state indefinitely.

2. **Lost dispatches (Scenario 2).** If the process crashes between the engine persisting `WORKER_SCHEDULED` and `K8sWorkerExecutionManager.submit()` creating the K8s Job, no Job exists. The engine's `DefaultWorkerExecutionRecoveryService` calls `schedulePersistedEvent(EventLog)` — a no-op for K8s. The work is permanently lost.

## Design Principle

K8s Jobs are externally durable — they survive process restarts. The K8s cluster IS the persistence layer. The in-memory `PendingCompletion` is a performance cache for the normal path, not the primary state. Make the Jobs self-describing for recovery and make the completion path resilient to the cache being empty.

## Solution

Four changes, each addressing a specific gap:

### 1. Enriched Job Labels

Add four labels to every K8s Job so they carry enough metadata for self-contained recovery.

**Current labels:**
| Label | Value |
|-------|-------|
| `app.kubernetes.io/managed-by` | `casehub` |
| `casehub.io/dispatch-id` | random UUID |
| `casehub.io/capability` | capability tag |
| `casehub.io/tenancy-id` | tenancy ID |

**Added labels:**
| Label | Value | Why |
|-------|-------|-----|
| `casehub.io/case-id` | case UUID | Currently only in env vars — not queryable by label selector |
| `casehub.io/worker-name` | worker name | Currently not on the Job at all |
| `casehub.io/event-log-id` | engine event log ID | Currently not on the Job at all |
| `casehub.io/idempotency` | inputDataHash (40-char hex SHA-1) | Currently only in env var `CASEHUB_IDEMPOTENCY` — label avoids navigating pod template spec for recovery |

All values fit K8s label constraints (≤63 chars, alphanumeric + `-_.`).

**Changes:**
- `K8sWorkerConstants` — four new label constants
- `K8sJobBuilder.build()` — gains `workerName`, `eventLogId`, and `idempotency` parameters (`caseId` is already a parameter, used for the `CASEHUB_CASE_ID` env var; it is additionally used as a label)
- `K8sJobBuilder.buildLabels()` — adds the four labels
- `K8sWorkerExecutionManager.submit()` — passes the new params (all are already available at the call site: `worker.name()`, `eventLogId`, `ctx.idempotency()`)

### 2. Eager Resolver Initialization

**Problem discovered during review:** `JobDefinitionResolver.initialize()` (which loads job definitions from Config) is never called in production code. The `K8sWorkerRuntime.initialize()` calls `resolver.capabilities()` but the underlying `definitions` map is `Map.of()` until `initialize()` runs. This is a pre-existing bug — the resolver initialization depends on test setup calling `resolver.initialize(Map.of(...))`.

Additionally, the engine's `WorkerRecoveryCoordinator` fires at `@Priority(22)` — well before `WorkerLifecycleOrchestrator` at `@Priority(APPLICATION + 10)` = 2010. Recovery subscribes to an async Uni (database query) at priority 22. If the query completes before the orchestrator initializes the K8s runtime, `CompositeWorkerExecutionManager.schedulePersistedEvent()` calls `backend.supports()` → `resolver.canResolve()` → `capabilities()` returns empty → recovery event silently dropped.

**Fix:** Add `@PostConstruct` to `JobDefinitionResolver` that calls `initialize()`. The resolver reads from `Config` (injected by CDI), which is available at bean creation time — no I/O, no external dependencies. This ensures:
- `capabilities()` returns correct results from the moment the bean is created
- `supports()` works before any startup observer fires
- The race between engine recovery (priority 22) and worker initialization (priority 2010) is eliminated — the resolver is ready before either runs
- `K8sWorkerRuntime.initialize()` can still call `resolver.capabilities()` as a readiness check (idempotent, no double-init concern since `definitions` is a volatile `Map.copyOf`)

**Changes:**
- `JobDefinitionResolver` — add `@PostConstruct void init() { initialize(); }`

### 3. Recovery Path in `processTerminal()`

When `registry.complete()` returns empty, reconstruct the correlation context from the Job itself instead of dropping the completion.

**At-most-once guard:** `K8sJobInformerManager` maintains a `Set<String> recoveredDispatchIds` (backed by `ConcurrentHashMap.newKeySet()`). The guard uses an atomic `add()` call: `recoveredDispatchIds.add(dispatchId)` returns `true` if the element was newly added, `false` if already present. This mirrors the `registry.complete()` atomicity — `ConcurrentHashMap.newKeySet().add()` delegates to `putIfAbsent()`, which is a single atomic operation with no TOCTOU window.

Without this guard, multiple informer callbacks for the same Job (onAdd → cleanup delete → onDelete, onAdd → onUpdate on metadata change, informer relist after 410 Gone) would each construct a fresh synthetic `PendingCompletion` and publish duplicate completions — bypassing the `registry.complete()` atomicity that protects the normal path.

**Recovery reconstruction steps:**
1. Attempt `recoveredDispatchIds.add(dispatchId)` — if returns `false` (already recovered), return early
2. Extract from Job labels: `caseId`, `workerName`, `capabilityTag`, `tenancyId`, `eventLogId`, `idempotency`
3. Load `CaseInstance` via `caseInstanceRepository.findByUuid(UUID, tenancyId).await().indefinitely()` — runs on the worker pool (dispatched via `runSubscriptionOn(Infrastructure.getDefaultWorkerPool())` in `handleTerminalEvent()`), blocking is safe
4. Construct `Worker`: `Worker.builder().name(workerName).capabilityName(capabilityTag).noFunction().build()` — the completion handler only uses `name()` and `capabilityNames()`
5. Resolve `provisionerMeta` from `JobDefinitionResolver.resolve(capabilityTag, tenancyId)` for `cleanup` and `maxOutputBytes`
6. Build synthetic `PendingCompletion` with `faultAddress = K8S_WORKER_FAULT`, `callbackToken = ""`, `registeredAt = Instant.now()`, `expiresAt = Instant.MAX`

After reconstruction, the existing completion/fault logic runs unchanged.

**Recovery failure cases:**
- CaseInstance not found → log warning, skip (case closed/deleted during downtime)
- Capability not resolvable → use defaults (`maxOutputBytes = 1048576`, `cleanup = DELETE`)
- Missing labels (pre-upgrade Jobs) → log warning, skip (cannot recover without metadata)
- CaseInstance has progressed beyond the worker step → the engine's existing completion handler applies output and calls `resumeIfWaiting()`. If the case is no longer waiting for this worker (progressed via manual signal, timeout, or parallel path), `resumeIfWaiting()` is a no-op — the case state is not corrupted. Output application to the working context is bounded and non-destructive. This is a platform-level concern affecting all async workers, not K8s-specific.

**New dependency:** `K8sJobInformerManager` injects `CaseInstanceRepository`. First time a worker module loads CaseInstances directly — justified because recovery has no dispatcher to hand them over.

**Helper method:** `recoverFromJob(Job, String dispatchId)` on `K8sJobInformerManager` encapsulates steps 1-6 (atomic guard, label extraction, CaseInstance lookup, Worker/PendingCompletion construction), returns `Optional<PendingCompletion>`. The caller (`processTerminal()`) just calls the helper and uses the result — no split guard logic between caller and helper.

### 4. `schedulePersistedEvent()` Implementation

Handles Scenario 2: no Job exists because the process crashed before Job creation. The engine's recovery service calls this existing SPI method (currently a default no-op for K8s).

**Implementation in `K8sWorkerExecutionManager`:**
1. Extract from `eventLog.getMetadata()`: `workerName`, `capabilityName`
2. Extract: `caseId = eventLog.getCaseId()`, `tenancyId = eventLog.tenancyId`, `eventLogId = eventLog.id`
3. Resolve `JobDefinition` from `capabilityName` + `tenancyId` via resolver
4. Query K8s: list Jobs with label selector `managed-by=casehub,case-id=<uuid>,capability=<tag>,worker-name=<name>` across configured namespaces
5. If matching Job exists (terminal or running) → return voidItem (informer handles it)
6. If no matching Job → re-dispatch:
   - Load `CaseInstance` reactively via `caseInstanceRepository.findByUuid(caseId, tenancyId)` — chain via `.chain()` on the returned Uni (no blocking; the method returns `Uni<Void>` and the composite chains it reactively)
   - Construct `Worker` from `workerName` + `capabilityName`
   - Construct `Capability` from `capabilityName` (empty schemas — dispatch doesn't use them)
   - Deserialize `inputData` from `eventLog.getPayload()`
   - Call `submit(eventLogId, caseInstance, worker, capability, inputData)` — this passes through the existing `maxInputBytes` validation, which is intentional: if config was lowered between restarts, recovery respects the current policy

**Edge cases:**
- CaseInstance not found → log warning, return voidItem (case closed)
- Capability not resolvable → log warning, return voidItem (config removed)
- K8s API unavailable → propagate exception (engine recovery handles failures gracefully)

**New dependency:** `K8sWorkerExecutionManager` injects `CaseInstanceRepository`.

## How Recovery Triggers

No separate recovery scan needed. The informer's existing `onAdd` event IS the scan:

- **Terminal Jobs at startup:** `onAdd` → `handleTerminalEvent()` → `processTerminal()` → `registry.complete()` empty → recovery path → completion/fault published
- **Running Jobs at startup:** `onAdd` → `handleTerminalEvent()` → `isTerminal()` false → returns. Later `onUpdate` → `processTerminal()` → recovery path → completion delivered
- **Lost dispatches:** Engine recovery finds orphaned `WORKER_SCHEDULED` → calls `schedulePersistedEvent()` → K8s checks cluster → no Job found → re-dispatches

## Startup Ordering

`WorkerRecoveryCoordinator` fires at `@Priority(22)`, subscribing to an async recovery Uni. `WorkerLifecycleOrchestrator` fires at `@Priority(APPLICATION + 10)` = 2010, calling `runtime.initialize().await().indefinitely()` — this blocks the startup thread until the K8s runtime's Uni completes (including `informerManager.start()`). Lower CDI priority fires first — recovery subscribes before the orchestrator runs.

This is safe because:
1. `JobDefinitionResolver` eagerly initializes from Config via `@PostConstruct` — `supports()` returns correct results before any startup observer fires, ensuring `CompositeWorkerExecutionManager.schedulePersistedEvent()` routes correctly to the K8s backend
2. The orchestrator blocks at priority 2010 until `K8sWorkerRuntime.initialize()` completes — `informerManager.start()` has returned and informers are created. However, the informer's initial list sync (which fires `onAdd` for existing Jobs) happens asynchronously after `start()` returns — the fabric8 `SharedIndexInformer.inform()` initiates the API server watch and returns immediately
3. Both the recovery Uni (database query) and the informer initial list sync execute asynchronously on background threads. In practice, the database query (which involves connection pool init on first use) takes longer than the informer's initial list response. Even if `schedulePersistedEvent()` runs before the informer syncs and returns voidItem for a terminal Job, the informer's eventual `onAdd` processes it once connected — the `ttlSecondsAfterFinished` (≥300s) provides the safety window

## Race Condition Safety

**Normal operation (no restart):** If both the informer and `schedulePersistedEvent` try to process the same Job, `registry.complete(dispatchId)` is atomic (`ConcurrentHashMap.remove`) — only one wins. The other gets empty and skips. No double-completion.

**After restart (registry lost):** Two distinct mechanisms prevent double-completion:
- **Informer double-fire** (onAdd/onDelete/onUpdate/relist for the same Job): The `recoveredDispatchIds` guard uses an atomic `add()` call — only the first thread succeeds; subsequent calls for the same `dispatchId` get `false` and return early. This mirrors the `registry.complete()` atomicity.
- **Informer vs. `schedulePersistedEvent`** (both attempt the same dispatch): `schedulePersistedEvent()` queries K8s for an existing Job (§4 step 4-5). If the Job exists (terminal or running), it returns voidItem and lets the informer handle completion. If the informer already deleted the Job (cleanup), `schedulePersistedEvent()` finds no match and re-dispatches a new Job with a new `dispatchId`. The engine deduplicates completions via `inputDataHash` — both the original and re-dispatched Jobs produce the same execution key (`caseId + "|" + workerId + "|" + inputDataHash`), so the second completion is deduplicated at the EventLog level.

## Startup Burst

If many terminal Jobs exist at startup, they all queue on the worker pool via the informer's `onAdd` events. Each recovery hit is one `CaseInstanceRepository.findByUuid()` call. Bounded by the number of managed Jobs, runs on worker threads — no event-loop risk.

## Pre-upgrade Jobs

Jobs dispatched before the label enrichment lack `casehub.io/case-id`, `casehub.io/worker-name`, `casehub.io/event-log-id`, `casehub.io/idempotency`.

**`processTerminal()` recovery:** Checks for these labels and logs a warning if missing — those Jobs cannot be recovered. They expire via `ttlSecondsAfterFinished` on the K8s side. This is acceptable — enrichment takes effect on the next dispatch.

**`schedulePersistedEvent()` duplicate risk:** The label selector includes the new labels (`case-id`, `worker-name`). Pre-upgrade running Jobs lack these labels, so the query finds no match and creates a duplicate Job. This is acceptable during the upgrade window:
- Duplicates are idempotent — the engine deduplicates completions via `inputDataHash` (the execution key is `caseId + "|" + workerId + "|" + inputDataHash`)
- The window is bounded — only Jobs dispatched before the upgrade are affected
- The duplicate Job wastes compute but does not corrupt state

## Files Changed

| File | Change |
|------|--------|
| `K8sWorkerConstants` | 4 new label constants |
| `K8sJobBuilder` | `build()` gains `workerName`, `eventLogId`, `idempotency` params; `buildLabels()` adds four labels; `caseId` already a param, now also used as label |
| `K8sWorkerExecutionManager` | Passes new params to builder; implements `schedulePersistedEvent()` reactively; injects `CaseInstanceRepository` |
| `K8sJobInformerManager` | `processTerminal()` recovery path; `recoverFromJob()` helper; injects `CaseInstanceRepository` |
| `JobDefinitionResolver` | `@PostConstruct` for eager Config initialization |

## Files Unchanged

- `AsyncWorkerCompletionRegistry` — stays in-memory, remains a performance cache
- `PendingCompletion` — record unchanged; recovery constructs synthetic instances
- `WorkerLifecycleOrchestrator` — no startup ordering changes needed
- `WorkerRuntime` / `K8sWorkerRuntime` — no new lifecycle methods
- `WorkflowCompletionPublisher` / `WorkerFaultPublisher` — unchanged; recovery uses same path
- All other worker modules — zero impact

## Non-Impacts

- **No engine-common SPI changes.** `schedulePersistedEvent(EventLog)` exists as a default method. We implement an existing contract.
- **No new module dependencies.** `CaseInstanceRepository` is in `engine-common`, already a workers-k8s dependency.
- **No database schema.** No Flyway migrations. No JPA entities. K8s is the durable store.

## Assumptions

- **A1:** `EventLog.metadata` always contains `capabilityName` for `WORKER_SCHEDULED` events. Verified at `WorkerScheduleEventHandler.java:179`. If null (corrupted or legacy EventLog), `CompositeWorkerExecutionManager.schedulePersistedEvent()` silently returns voidItem — this is a pre-existing engine behavior, not introduced by this spec. Recommend adding WARN logging in the engine composite for observability (out of scope for this spec).

## Test Strategy

### Unit Tests

1. **`recoverFromJob()` — label extraction:**
   - All labels present → returns `Optional<PendingCompletion>` with correct fields
   - Missing `case-id` label → returns empty, logs warning
   - Missing `idempotency` label → returns empty, logs warning
   - CaseInstance not found → returns empty, logs warning
   - Capability not resolvable → returns PendingCompletion with default provisionerMeta

2. **`processTerminal()` — recovery path integration:**
   - Registry has PendingCompletion → normal path (no recovery)
   - Registry empty, recovery succeeds → completion published with correct idempotency
   - Registry empty, recovery fails (missing labels) → no completion, no exception
   - Second `processTerminal()` call for same dispatchId after recovery → `recoveredDispatchIds` guard prevents duplicate completion
   - Informer relist (onAdd for already-recovered Job) → no completion published

3. **`schedulePersistedEvent()` — K8s query and re-dispatch:**
   - No matching Job → re-dispatches via `submit()`, returns voidItem
   - Running Job found → returns voidItem (informer handles it)
   - Terminal Job found → returns voidItem (informer handles it)
   - CaseInstance not found → logs warning, returns voidItem
   - Capability not resolvable → logs warning, returns voidItem
   - K8s API failure → propagates exception

4. **`K8sJobBuilder` — label generation:**
   - New labels present with correct values
   - Label values respect K8s constraints (≤63 chars)

### Integration Tests

5. **End-to-end recovery flow:**
   - Create Job with enriched labels → simulate restart (clear registry) → informer fires `onAdd` for terminal Job → completion published → case progresses

6. **Race condition — concurrent processTerminal and schedulePersistedEvent:**
   - Both attempt the same dispatch → only one produces a completion → no double-processing

### Pre-upgrade Compatibility

7. **Pre-upgrade Job in processTerminal:**
   - Job with only old labels → logs warning, skips recovery
8. **Pre-upgrade Job in schedulePersistedEvent:**
   - Running pre-upgrade Job not found by new-label selector → duplicate Job created → engine deduplicates on completion
