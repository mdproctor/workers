# bindingName Propagation Through Worker Completion Path

**Issue:** casehubio/workers#18
**Blocked by:** casehubio/engine#676 (add `bindingName` to `WorkerExecutionManager.submit()`)
**Date:** 2026-07-06
**Status:** Approved

## Problem

`WorkflowCompletionPublisher.complete()` always passes `null` for `bindingName` in `WorkflowExecutionCompleted.approved()`. When `bindingName` is null, the engine's `WorkflowExecutionCompletedHandler` falls back to `findMatchingCapabilityBinding()` which iterates all bindings looking for a match. This selects the wrong binding when a case definition has multiple bindings for the same capability.

The `WORKER_SCHEDULED` EventLog metadata contains `bindingName` (set by `WorkerScheduleEventHandler`), but it is never threaded through to the completion path. The gap is in `WorkerExecutionManager.submit()` which lacks a `bindingName` parameter.

## Scope

All 6 worker modules — HTTP, MCP, Camel, Script, GitHub Actions, K8s. The infrastructure change is in workers-common; K8s adds annotation persistence and recovery path changes on top.

Issue #18's title ("K8s: propagate bindingName through worker completion path") should be updated to reflect the full scope across all worker modules.

## Approach

Add `bindingName` to `WorkerCorrelationContext`. Since `PendingCompletion` holds `WorkerCorrelationContext`, and all completion/callback/recovery paths flow through `PendingCompletion.correlationContext()`, the field propagates automatically to every path that publishes `WorkflowExecutionCompleted`.

## Design

### 1. WorkerCorrelationContext

Add `bindingName` as a nullable String (last position):

```java
public record WorkerCorrelationContext(
    CaseInstance caseInstance,
    Worker worker,
    String idempotency,
    String tenancyId,
    String bindingName
) {}
```

### 2. WorkflowCompletionPublisher

Replace `null` with `ctx.bindingName()`:

```java
public void complete(WorkerCorrelationContext ctx, Map<String, Object> output) {
    eventBus.publish(EventBusAddresses.WORKER_EXECUTION_FINISHED,
        WorkflowExecutionCompleted.approved(
            ctx.caseInstance(), ctx.worker(), ctx.idempotency(), output, ctx.bindingName()));
}
```

### 3. WorkerFaultEvent and WorkerFaultPublisher

Add `bindingName` as a nullable String to `WorkerFaultEvent`:

```java
public record WorkerFaultEvent(
    CaseInstance caseInstance,
    Worker worker,
    Capability capability,
    String inputDataHash,
    String eventLogId,
    Throwable cause,
    String bindingName) {}
```

Update both `WorkerFaultPublisher.fault()` overloads to include `bindingName` from the correlation context:

```java
public void fault(String faultAddress, WorkerCorrelationContext ctx,
                  Capability capability, Long eventLogId, Throwable cause) {
    eventBus.publish(faultAddress, new WorkerFaultEvent(
        ctx.caseInstance(), ctx.worker(), capability,
        ctx.idempotency(), eventLogId.toString(), cause, ctx.bindingName()));
}

public void fault(PendingCompletion pending, Throwable cause) {
    eventBus.publish(pending.faultAddress(), new WorkerFaultEvent(
        pending.correlationContext().caseInstance(),
        pending.correlationContext().worker(),
        pending.capability(),
        pending.correlationContext().idempotency(),
        pending.eventLogId().toString(),
        cause,
        pending.correlationContext().bindingName()));
}
```

This ensures `bindingName` flows through fault events to the `WorkerFaultHandler`, where it is used for both retry re-dispatch and retries-exhausted publishing.

### 4. SPI Override Pattern (all 6 workers)

Each worker's execution manager overrides both `submit()` signatures. The old 5-arg delegates to the new 6-arg with `null`:

```java
@Override
public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                        Capability capability, Map<String, Object> inputData) {
    return submit(eventLogId, instance, worker, capability, inputData, null);
}

@Override
public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                        Capability capability, Map<String, Object> inputData,
                        String bindingName) {
    // current submit() body, buildCtx receives bindingName
}
```

Each worker's `buildCtx()` (or equivalent) gains a `bindingName` parameter passed to the `WorkerCorrelationContext` constructor.

Until engine#676 ships, the engine calls the 5-arg overload → delegates to 6-arg with null → identical to today. Once the engine calls the 6-arg directly, `bindingName` flows through.

### 5. WorkerFaultHandler

`handleFault()` uses `event.bindingName()` (propagated via `WorkerFaultEvent` from §3) in both paths:

- **Retry re-dispatch:** `reloadAndResubmit()` passes `event.bindingName()` to the 6-arg `submit()`:

```java
workerExecutionManager.submit(
    Long.parseLong(event.eventLogId()),
    event.caseInstance(), event.worker(), event.capability(), inputData,
    event.bindingName());
```

- **Retries exhausted:** both the `PermanentFaultException` and max-attempts branches pass `event.bindingName()` to `publishRetriesExhausted()`:

```java
retrySupport.publishRetriesExhausted(
    instance.getUuid(), worker.name(), inputDataHash,
    event.bindingName(), tenancyId);
```

This replaces the current `worker.name()` surrogate with the explicitly-propagated value, consistent with the completion path.

### 6. K8s-specific

**Constant:** `K8sWorkerConstants.BINDING_NAME_ANNOTATION = "casehub.io/binding-name"`.

**Job builder:** `K8sJobBuilder.build()` gains a `bindingName` parameter. Adds a K8s **annotation** (not label) when non-null, omits when null. Annotations have no length limit, unlike labels (63-character max). Since `bindingName` is only read during recovery — not used in label-selector queries — an annotation is the correct K8s primitive.

**Recovery:** `K8sJobInformerManager.recoverFromJob()` reads `annotations.get(BINDING_NAME_ANNOTATION)` and passes to `WorkerCorrelationContext`. Pre-upgrade Jobs without the annotation recover with `bindingName = null`.

**schedulePersistedEvent:** `K8sWorkerExecutionManager.schedulePersistedEvent()` reads `bindingName` from EventLog metadata, calls the 6-arg `submit()`.

### 7. Paths that require no changes

These flow through `PendingCompletion.correlationContext()` and get `bindingName` automatically:

- `WorkerCallbackResource` (REST callback)
- `CasehubProducer` (Camel component)
- `WorkerCompletionExpiryObserver`
- `WorkerFaultCallbackObserver`
- `K8sJobInformerManager.processTerminal()` (completion and fault paths)

## Engine Dependency

casehubio/engine#676:
1. Add a default method overload to `WorkerExecutionManager.submit()` with `bindingName` parameter. The default delegates to the existing 5-arg `submit()`.
2. Override the 6-arg `submit()` in `CompositeWorkerExecutionManager` with routing logic identical to its 5-arg override, calling `selected.get().submit(eventLogId, instance, worker, capability, inputData, bindingName)`.
3. Update `WorkerScheduleEventHandler.submitIfNeeded()` to call the 6-arg overload with `bindingName` from the `WorkerScheduleEvent`.
4. Add `CompositeWorkerExecutionManagerTest` coverage: 6-arg routes `bindingName` to selected backend's 6-arg, null `bindingName` routes correctly.

`QuartzWorkerExecutionManager` does not need a 6-arg override — it reads `bindingName` from EventLog metadata independently via `QuartzWorkerSchedulerService`, never from the `submit()` parameter.

Workers-side code compiles and passes tests without the engine change — `bindingName` is null until the engine calls the new overload.

## Files Changed

### workers-common (production)
- `WorkerCorrelationContext.java` — add `bindingName` field
- `WorkflowCompletionPublisher.java` — pass `ctx.bindingName()`
- `WorkerFaultEvent.java` — add `bindingName` field
- `WorkerFaultPublisher.java` — pass `ctx.bindingName()` in both `fault()` overloads
- `WorkerFaultHandler.java` — use `event.bindingName()` for retry re-dispatch and exhaustion

### workers-http
- `HttpWorkerExecutionManager.java` — override 6-arg `submit()`, update `buildCtx()`

### workers-mcp
- `McpWorkerExecutionManager.java` — override 6-arg `submit()`, update context construction

### workers-camel
- `CamelWorkerExecutionManager.java` — override 6-arg `submit()`, update context construction

### workers-script
- `ScriptWorkerExecutionManager.java` — override 6-arg `submit()`, update `buildCtx()`

### workers-github-actions
- `GitHubActionsWorkerExecutionManager.java` — override 6-arg `submit()`, update context construction

### workers-k8s
- `K8sWorkerConstants.java` — add `BINDING_NAME_ANNOTATION`
- `K8sJobBuilder.java` — add `bindingName` parameter, add annotation
- `K8sWorkerExecutionManager.java` — override 6-arg `submit()`, update `buildCtx()`, update `schedulePersistedEvent()`
- `K8sJobInformerManager.java` — read annotation in `recoverFromJob()`

### Tests
- `WorkerCorrelationContextTest` — verify new field including null
- `WorkflowCompletionPublisherTest` — verify bindingName flows to completion event
- `WorkerFaultEventTest` — verify new field including null
- `WorkerFaultPublisherTest` — verify bindingName flows from context to fault event
- `WorkerFaultHandlerTest` — verify `event.bindingName()` used in retry re-dispatch and exhaustion
- Per-module execution manager tests (6 modules) — update ctx construction, verify 6-arg override
- `K8sJobBuilderTest` — verify annotation present/absent based on bindingName
- `K8sJobInformerManagerTest` — verify recovery reads annotation, pre-upgrade Job recovers with null
- `K8sWorkerExecutionManagerTest` — verify schedulePersistedEvent reads metadata

## Backward Compatibility

- `bindingName = null` throughout until engine#676 ships — identical to current behavior
- Pre-upgrade K8s Jobs without `casehub.io/binding-name` annotation recover with null — graceful degradation
- Old `submit()` 5-arg signature still works via delegation — no callers break
