# workers-script Design Spec

**Issue:** casehubio/casehub-workers#9
**Date:** 2026-06-16
**Status:** Approved (rev 3)
**Review cycles:** 3 — 16 findings total, all accepted (2 with scope corrections)

---

## Summary

`workers-script` dispatches case steps to local subprocesses — shell scripts, Python scripts, CLI tools. Config-driven named script definitions map capability tags (`script:<name>`) to executables with arguments, working directory, environment, and timeout. Execution is synchronous: start the process, pipe inputData as JSON via stdin, wait for exit, capture stdout/stderr, complete or fault.

A prerequisite extraction of the fault pipeline infrastructure to workers-common is completed before the script module is created (§2).

---

## 2. Prerequisite: Extract Fault Pipeline to workers-common

The fault pipeline — publisher, handler body, and CDI event observers — is duplicated across all existing worker modules. Adding workers-script would create the 5th copy. This extraction follows the precedent set by the GitHub Actions spec (§2: "Extract Fault Exception Types to workers-common") and the runtime lifecycle spec (lifecycle SPI extraction).

### 2.1 What is duplicated

**Fault publishers** (4 copies: HTTP, Camel, MCP, GitHub Actions) — identical except for the event bus address constant. Each has a `fault(ctx, capability, eventLogId, cause)` method that constructs `WorkflowExecutionFailed` and publishes to a per-module address. HTTP and Camel additionally have a `fault(PendingCompletion, cause)` overload for async mode.

**Fault event handler bodies** (4 copies) — the `onFault()` method body is structurally identical: persist → PermanentFaultException check → count attempts → retry-or-exhaust. The `@ConsumeEvent` annotation requires a compile-time constant, so per-module stub classes must remain, but the ~90-line body can be extracted.

**Known bug:** `CamelWorkerFaultEventHandler` is missing the `PermanentFaultException` check and the `RetryAfterException` check — it retries permanent faults up to `maxAttempts` times. This bug exists because the Camel handler was written (June 8) before `PermanentFaultException` was extracted to workers-common (June 10). Extracting the handler body fixes this for all workers.

**CDI event observers** (2 copies each: HTTP and Camel only) — `CompletionExpiryObserver` and `FaultCallbackObserver` differ only by the WORKER_TYPE filter and the fault publisher reference. MCP and GitHub Actions are sync-only and don't have these observers.

### 2.2 Extraction scope

**New classes in workers-common:**

| Class | Replaces | Purpose |
|-------|----------|---------|
| `WorkerFaultPublisher` | 4 per-module publishers | Generic, parameterized by fault address. Two overloads: `fault(faultAddress, ctx, capability, eventLogId, cause)` and `fault(faultAddress, pending, cause)` |
| `WorkerFaultHandler` | 4 × ~90-line handler bodies | Shared fault handler logic: persist → PermanentFaultException check → count → RetryAfterException check → retry-or-exhaust. Always uses `emitOn(Infrastructure.getDefaultWorkerPool())` before re-dispatch — one unnecessary thread hop for reactive workers (HTTP, MCP, GitHub Actions) is negligible during error handling, and correctness regardless of what any `submit()` does internally is worth more than one thread hop on the error path |
| `WorkerCompletionExpiryObserver` | 2 per-module expiry observers | Single generic observer — routes via `faultAddress` from `PendingCompletion` |
| `WorkerFaultCallbackObserver` | 2 per-module callback observers | Single generic observer — routes via `faultAddress` from `PendingCompletion` |

**Changes to existing workers-common types:**

| Type | Change | Rationale |
|------|--------|-----------|
| `PendingCompletion` | Add `faultAddress` field | Enables generic observers to route without a registry — each pending completion carries its own fault address |
| `AsyncWorkerCompletionRegistry.register()` | Add `faultAddress` parameter | Populated by the calling execution manager (which already knows its fault address via its constants class) |

**Deleted from per-module packages:**

| Deleted class | Module |
|---------------|--------|
| `HttpWorkerFaultPublisher` | workers-http |
| `CamelWorkerFaultPublisher` | workers-camel |
| `McpWorkerFaultPublisher` | workers-mcp |
| `GitHubActionsWorkerFaultPublisher` | workers-github-actions |
| `HttpCompletionExpiryObserver` | workers-http |
| `CamelCompletionExpiryObserver` | workers-camel |
| `HttpFaultCallbackObserver` | workers-http |
| `CamelFaultCallbackObserver` | workers-camel |

8 classes deleted.

**Shrunk per-module stubs** (remain due to CDI `@ConsumeEvent` compile-time constant constraint):

```java
// Per-module — e.g. ScriptWorkerFaultEventHandler
@ConsumeEvent(value = ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT, blocking = true)
public Uni<Void> onFault(WorkflowExecutionFailed event) {
    return workerFaultHandler.handleFault(event);
}
```

Each shrinks from ~90 lines to ~5 lines across HTTP, Camel, MCP, GitHub Actions, and script.

### 2.3 Bug fixes included

The shared `WorkerFaultHandler.handleFault()` includes the `PermanentFaultException` guard and `RetryAfterException` check. This fixes the Camel fault handler which currently lacks both.

---

## 3. Module Structure

- **Module:** `workers-script`
- **Artifact:** `casehub-workers-script`
- **Root package:** `io.casehub.workers.script`
- **workerType discriminator:** `"script"`

**Dependencies:** `casehub-workers-common`, `casehub-engine-api`, `casehub-engine-common`, `quarkus-arc`, `quarkus-vertx`. No external dependencies beyond what workers-common already provides — `ProcessBuilder` is `java.lang`.

---

## 4. Config Model

**Namespace:** `casehub.workers.script`

### Module-level defaults

| Property | Type | Default | Purpose |
|----------|------|---------|---------|
| `casehub.workers.script.default-timeout-seconds` | int | 300 | Per-process timeout when not overridden per-script |
| `casehub.workers.script.max-output-bytes` | long | 1048576 (1 MB) | Stdout/stderr capture cap when not overridden per-script |

### Per-script definitions

Config path: `casehub.workers.script.scripts.<name>.*`

Capability tag: `script:<name>` (e.g. `script:data-pipeline`)

| Property | Type | Required | Default | Purpose |
|----------|------|----------|---------|---------|
| `command` | String | yes | — | Executable path or name (e.g. `python3`, `/opt/scripts/run.sh`) |
| `args` | List\<String\> | no | empty | Static argument list, comma-separated in config |
| `working-directory` | String | no | system temp dir | Process working directory |
| `environment` | Map\<String, String\> | no | empty | Extra env vars merged onto process environment |
| `timeout-seconds` | int | no | module default | Per-script timeout override |
| `max-output-bytes` | long | no | module default | Per-script output cap override |

**Example:**

```properties
casehub.workers.script.default-timeout-seconds=300
casehub.workers.script.max-output-bytes=1048576

casehub.workers.script.scripts.data-pipeline.command=python3
casehub.workers.script.scripts.data-pipeline.args=/opt/scripts/pipeline.py,--mode,batch
casehub.workers.script.scripts.data-pipeline.working-directory=/opt/data
casehub.workers.script.scripts.data-pipeline.environment.PYTHONPATH=/opt/lib
casehub.workers.script.scripts.data-pipeline.timeout-seconds=600
casehub.workers.script.scripts.data-pipeline.max-output-bytes=2097152
```

---

## 5. Key Types

### ScriptDefinition (record)

```java
record ScriptDefinition(
    String name,
    String command,
    List<String> args,
    String workingDirectory,        // nullable → system temp
    Map<String, String> environment, // merged onto process env
    int timeoutSeconds,             // fallback to module default
    long maxOutputBytes             // fallback to module default
)
```

### ScriptDefinitionResolver

Implements `WorkerCapabilityResolver<ScriptDefinition>`. Reads config at `initialize()`, builds an in-memory map of `script:<name>` → `ScriptDefinition`. Same pattern as `HttpEndpointResolver` and `McpServerResolver`.

---

## 6. Class Inventory

| Class | Responsibility |
|-------|---------------|
| `ScriptWorkerConstants` | `WORKER_TYPE = "script"` |
| `ScriptWorkerEventBusAddresses` | `SCRIPT_WORKER_FAULT = "casehub.workers.script.fault"` |
| `ScriptDefinition` | Record — command, args, workingDirectory, environment, timeoutSeconds, maxOutputBytes |
| `ScriptDefinitionResolver` | `WorkerCapabilityResolver<ScriptDefinition>` — config-driven, populated at init |
| `ScriptWorkerRuntime` | `WorkerRuntime` impl — delegates `initialize()` to resolver, reports capabilities. Zero scripts configured → FAULTED (matches MCP and GitHub Actions pattern) |
| `ScriptReactiveWorkerProvisioner` | Capability probe — validates tag exists, command non-blank. Returns `ProvisionResult.empty()` |
| `ScriptWorkerExecutionManager` | Core dispatch — ProcessBuilder, stdin, stdout/stderr capture, exit code classification. Owns a dedicated `ExecutorService` for stderr draining (see §7). `@PostConstruct` creates it, `@PreDestroy` shuts it down. `schedulePersistedEvent()` → no-op (`Uni.createFrom().voidItem()`). `getActiveWorkCount()` → 0 (sync execution, no async tracking) |
| `ScriptWorkerFaultEventHandler` | `@ConsumeEvent(SCRIPT_WORKER_FAULT, blocking=true)` — 5-line stub delegating to `WorkerFaultHandler` (from workers-common §2) |

8 classes (7 production + 1 record). The fault publisher, completion expiry observer, and fault callback observer are provided by workers-common generics (§2).

---

## 7. Execution Flow

The execution manager wraps the entire blocking subprocess lifecycle in a `Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())` — the same Mutiny primitive used by the Camel worker for its blocking `ProducerTemplate`. This ensures the blocking `Process.waitFor()` and stream reads run on the worker pool, not the Vert.x event loop.

```java
return Uni.createFrom()
    .item(() -> {
        // Steps 4-9 below — all blocking, all on worker pool
        Process process = buildAndStart(definition, inputData, ctx);
        // ... drain streams, waitFor, classify, return output
    })
    .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
    .flatMap(output -> {
        completionPublisher.complete(ctx, output);
        return Uni.createFrom().voidItem();
    })
    .onFailure().recoverWithUni(t -> {
        faultPublisher.fault(SCRIPT_WORKER_FAULT, ctx, capability, eventLogId, t);
        return Uni.createFrom().voidItem();
    });
```

### Step-by-step

```
submit(eventLogId, instance, worker, capability, inputData)
  │
  ├─ 1. Resolve: scriptDefinitionResolver.resolve(capability.getName())
  │     Catch WorkerProvisioningException, wrap in PermanentFaultException
  │     (config problem at dispatch time is not transient — matches MCP pattern)
  │
  ├─ 2. Build context: WorkerCorrelationContext(instance, worker, idempotencyHash, tenancyId)
  │
  ├─ 3. runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
  │     Everything below runs on the worker pool
  │
  ├─ 4. Build ProcessBuilder:
  │     command = [definition.command()] + definition.args()
  │     workingDirectory = definition.workingDirectory() ?? System.tmpdir
  │     environment = process env + definition.environment()
  │                   + CASEHUB_CASE_ID, CASEHUB_TENANCY_ID,
  │                     CASEHUB_CAPABILITY, CASEHUB_IDEMPOTENCY
  │     redirectErrorStream = false
  │
  ├─ 5. Start process, write inputData JSON to stdin, close stdin
  │     IOException from start() → PermanentFaultException (command not found)
  │
  ├─ 6. Drain stdout + stderr concurrently (bounded by maxOutputBytes)
  │     See "Stream Draining" section below
  │
  ├─ 7. process.waitFor(timeoutSeconds, SECONDS)
  │     timeout expired → process.destroyForcibly()
  │                      → process.waitFor() (reap zombie)
  │                      → PermanentFaultException
  │
  ├─ 8. Exit code classification:
  │     0 → parse stdout as JSON object → output map
  │         (only JSON objects produce structured output;
  │          JSON arrays, primitives, and invalid JSON
  │          fall through to raw wrapper)
  │         non-JSON → {"stdout": "<raw>", "stderr": "<raw>", "exitCode": 0}
  │     non-zero → RuntimeException (retryable)
  │
  └─ 9. completionPublisher.complete(ctx, output)
```

### Input Data Delivery

**Primary channel: stdin.** `inputData` is serialized as JSON and piped to the process's stdin. Language-agnostic, no size limits.

**Supplementary context: environment variables.**

| Env var | Source |
|---------|--------|
| `CASEHUB_CASE_ID` | `instance.getUuid().toString()` |
| `CASEHUB_TENANCY_ID` | `instance.tenancyId` |
| `CASEHUB_CAPABILITY` | `capability.getName()` |
| `CASEHUB_IDEMPOTENCY` | idempotency hash |

### Output Handling

On exit code 0:
1. Attempt to parse stdout as a JSON object using `TypeReference<Map<String, Object>>`. Only JSON objects produce structured output.
2. If parsing fails (invalid JSON, JSON array, JSON primitive) → wrap as `{"stdout": "<raw>", "stderr": "<raw>", "exitCode": 0}`

Stdout and stderr are each capped at `maxOutputBytes`. Content beyond the cap is truncated (see Stream Draining below).

### Stream Draining

`Process.waitFor()` deadlocks if the child fills the OS pipe buffer (~64KB on Linux) and nobody is reading. Both stdout and stderr must be drained concurrently.

**Thread model:** Read stdout on the current thread (already on the Mutiny worker pool via `runSubscriptionOn`). Spawn one `CompletableFuture` on a dedicated `ExecutorService` (cached thread pool, `@PostConstruct` on the execution manager, named threads `script-stderr-drain-N`) for stderr. This gives concurrent draining with one additional thread per dispatch instead of two, and avoids polluting `ForkJoinPool.commonPool()` with blocking I/O that can last up to `timeoutSeconds`.

**Executor lifecycle:** Created at `@PostConstruct`, shut down at `@PreDestroy` on `ScriptWorkerExecutionManager`. The executor belongs to the execution manager — the execution manager manages it. `@PreDestroy` calls `executor.shutdownNow()`.

**Bounded read loop:**

```
read into fixed buffer (8192 bytes)
while (bytesRead != -1 AND totalCaptured < maxOutputBytes):
    write min(bytesRead, maxOutputBytes - totalCaptured) to output
    read next buffer
// Cap reached — continue reading and discarding to prevent pipe deadlock
while (bytesRead != -1):
    read next buffer (discard)
```

The "drain remaining" phase after hitting `maxOutputBytes` is required — closing the stream would SIGPIPE the child process. The child runs to natural completion (or timeout); we simply discard output beyond the cap.

### Concurrency

Concurrent script dispatch is implicitly bounded by the Mutiny worker pool (default: 2 × CPU cores in Quarkus). Each dispatch blocks a worker-pool thread for the full process lifetime (up to `timeoutSeconds`). No additional concurrency limiting is implemented.

If script + Camel workers are co-deployed (after engine#461), they share the worker pool — a long-running script (600s timeout) blocks a thread that Camel also needs. This is acceptable for v1; a dedicated executor for script dispatch can be introduced if contention is observed.

### Security Model

Command, args, workingDirectory, and environment all come from operator-controlled configuration. InputData only reaches the process via stdin (JSON serialization), never via command-line arguments or environment variable interpolation. There is no path from user-supplied case data to command construction.

---

## 8. Fault Classification

### Permanent faults (PermanentFaultException — no retry)

- Script definition not found at dispatch time (catch `WorkerProvisioningException`, wrap in `PermanentFaultException` — config problem, not transient)
- Command executable not found (`IOException` from `ProcessBuilder.start()`)
- Working directory doesn't exist (validated before start)
- Process timeout exceeded (`destroyForcibly()`, then `waitFor()` to reap zombie)

**Timeout rationale:** This diverges from HTTP/MCP/GitHub Actions workers where WebClient timeouts are retryable (`RuntimeException`). For subprocesses, the divergence is deliberate: a script that consumed the full timeout period will almost certainly timeout again on retry, and each retry burns an OS process + worker-pool thread for the full duration with near-zero chance of success. Operator intervention is required — either increase the timeout or fix the script.

### Retryable faults (RuntimeException — enters retry pipeline)

- Any non-zero exit code
- `IOException` during stdin write or stdout/stderr read (transient I/O)

### Fault handling

Delegated to the shared `WorkerFaultHandler` in workers-common (§2). The per-module `ScriptWorkerFaultEventHandler` is a 5-line stub. The shared handler always uses `emitOn(Infrastructure.getDefaultWorkerPool())` before re-dispatch — correct for all workers regardless of whether their `submit()` is blocking or reactive.

---

## 9. Runtime Initialization

`ScriptWorkerRuntime.initialize()` delegates to `ScriptDefinitionResolver.initialize()`. After initialization:

- If resolver reports zero scripts → `status = FAULTED` (matches MCP pattern: no MCP servers → FAULTED, and GitHub Actions pattern: no token → FAULTED). A script worker on the classpath with no scripts configured is a misconfiguration.
- If resolver succeeds with one or more scripts → `status = RUNNING`
- `FAULTED → RUNNING` recovery: calling `initialize()` on a FAULTED runtime retries initialization

---

## 10. Co-deployment

Same constraints as all other workers:
- `workerType = "script"` discriminator in `PendingCompletion` prevents CDI event cross-talk
- `WorkerExecutionManager` CDI ambiguity with other worker modules — blocked by engine#461 (composite manager)
- CDI event observers (generic, in workers-common) filter by `faultAddress` from `PendingCompletion` — no per-module filter needed

---

## 11. Testing Strategy

Unit tests per class, using real processes rather than mocking ProcessBuilder:

| Test | Coverage |
|------|----------|
| `ScriptDefinitionResolverTest` | Config parsing, initialize(), resolve() known/unknown, firstMatch(), capabilities() |
| `ScriptReactiveWorkerProvisionerTest` | Provision valid/invalid capabilities, getCapabilities() delegation |
| `ScriptWorkerRuntimeTest` | PENDING→RUNNING, PENDING→FAULTED (no scripts), RUNNING no-op, FAULTED→RUNNING recovery, shutdown |
| `ScriptWorkerExecutionManagerTest` | JSON object stdout, JSON array stdout falls through to raw wrapper, non-JSON stdout fallback, non-zero exit fault, timeout kills process, stdin delivery, env vars, working directory, max-output-bytes truncation (with drain-past-cap), command-not-found permanent fault |
| `ScriptWorkerFaultEventHandlerTest` | Delegates to WorkerFaultHandler (stub verification) |

Prerequisite extraction tests (workers-common):

| Test | Coverage |
|------|----------|
| `WorkerFaultPublisherTest` | Publishes to parameterized address, both overloads |
| `WorkerFaultHandlerTest` | Permanent fault skips retry, retryable retries with backoff, RetryAfterException overrides delay, retries exhausted, emitOn before re-dispatch |
| `WorkerCompletionExpiryObserverTest` | Routes via faultAddress from PendingCompletion |
| `WorkerFaultCallbackObserverTest` | Routes via faultAddress from PendingCompletion |

Tests use `/bin/sh -c` with commands like `echo`, `cat`, `false`, `sleep` — works on macOS and Linux (CI).

---

## 12. Out of Scope

- Sandboxing / containerized execution (future concern — separate worker type)
- Remote script execution (use HTTP or MCP worker)
- Interactive / stdin-based processes (stdin is consumed by inputData delivery)
- Windows support (CaseHub deployment target is Linux/macOS)
