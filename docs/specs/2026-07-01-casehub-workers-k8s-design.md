# workers-k8s — Kubernetes Job Dispatch Worker

**Issue:** #10  
**Date:** 2026-07-01  
**Status:** Approved  
**Follow-up:** #16 (multi-cluster support), #17 (registry persistence for restart recovery)

---

## Summary

New worker module that dispatches CaseHub case steps as Kubernetes Jobs via the fabric8 client API. The fundamental difference from all existing workers is the **completion model**: watch-based observation via a shared fabric8 informer, rather than synchronous response or callback-based completion.

---

## Module Structure

| Attribute | Value |
|---|---|
| Folder | `workers-k8s` |
| ArtifactId | `casehub-workers-k8s` |
| Package | `io.casehub.workers.k8s` |
| Capability tag prefix | `k8s:` |
| Worker type discriminator | `"k8s"` |

### Dependencies

```xml
<dependencies>
    <dependency><groupId>io.casehub</groupId><artifactId>casehub-workers-common</artifactId></dependency>
    <dependency><groupId>io.casehub</groupId><artifactId>casehub-engine-api</artifactId></dependency>
    <dependency><groupId>io.casehub</groupId><artifactId>casehub-engine-common</artifactId></dependency>
    <dependency><groupId>io.casehub</groupId><artifactId>casehub-worker-api</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-arc</artifactId></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-kubernetes-client</artifactId></dependency>
    <!-- test -->
    <dependency><groupId>io.casehub</groupId><artifactId>casehub-workers-testing</artifactId><scope>test</scope></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-test-kubernetes-client</artifactId><scope>test</scope></dependency>
    <dependency><groupId>io.quarkus</groupId><artifactId>quarkus-junit</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.assertj</groupId><artifactId>assertj-core</artifactId><scope>test</scope></dependency>
</dependencies>
```

`quarkus-vertx` is NOT a direct dependency — fabric8 7.x uses Vert.x as its HTTP transport (transitive), and `workers-common` provides EventBus.

### Type Inventory

| Type | Role |
|---|---|
| `K8sWorkerConstants` | `WORKER_TYPE = "k8s"` |
| `K8sWorkerEventBusAddresses` | `K8S_WORKER_FAULT = "casehub.workers.k8s.fault"` |
| `JobDefinition` | Config record — image/template, namespace, resources, timeout |
| `CleanupPolicy` | `DELETE` / `RETAIN` enum |
| `JobDefinitionResolver` | `WorkerCapabilityResolver<JobDefinition>` — config-driven, single-tier |
| `K8sJobBuilder` | Builds fabric8 `Job` from `JobDefinition` + dispatch context |
| `K8sJobInformerManager` | Shared informer lifecycle — start, stop, event routing |
| `K8sJobOutputCapture` | Reads Pod logs after completion, parses as JSON |
| `K8sWorkerExecutionManager` | `@WorkerBackend @Priority(10)` — creates Job, registers pending |
| `K8sReactiveWorkerProvisioner` | Capability probe — validates tag exists in resolver |
| `K8sWorkerRuntime` | Lifecycle — starts informers, validates cluster connectivity |
| `K8sWorkerFaultEventHandler` | 5-line stub delegating to `WorkerFaultHandler` |

---

## Job Definition and Configuration

### `JobDefinition` Record

```java
record JobDefinition(
    String name,
    String namespace,
    String image,
    List<String> command,
    List<String> args,
    String template,            // classpath resource path; null → build from image fields
    String cpuRequest,
    String cpuLimit,
    String memoryRequest,
    String memoryLimit,
    int timeoutSeconds,         // maps to K8s activeDeadlineSeconds
    int ttlAfterFinished,       // safety-net TTL in seconds (default 600, minimum 300)
    int backoffLimit,           // K8s-level retry before surfacing to CaseHub (default 0)
    long maxOutputBytes,        // per-job output capture limit (default 1048576)
    String serviceAccount,
    Map<String, String> labels,
    Map<String, String> environment,
    CleanupPolicy cleanup       // DELETE (default) or RETAIN
)
```

### `CleanupPolicy` Enum

```java
enum CleanupPolicy { DELETE, RETAIN }
```

### Configuration

Global defaults + per-job override via MicroProfile Config:

```properties
# Global defaults
casehub.workers.k8s.namespace=default
casehub.workers.k8s.timeout-seconds=3600
casehub.workers.k8s.ttl-after-finished=600
casehub.workers.k8s.backoff-limit=0
casehub.workers.k8s.cleanup=delete
casehub.workers.k8s.max-output-bytes=1048576
casehub.workers.k8s.max-input-bytes=262144

# Image-based job
casehub.workers.k8s.jobs.report-gen.image=acme/report-gen:latest
casehub.workers.k8s.jobs.report-gen.namespace=batch
casehub.workers.k8s.jobs.report-gen.command=python,generate.py
casehub.workers.k8s.jobs.report-gen.timeout-seconds=3600
casehub.workers.k8s.jobs.report-gen.cpu-limit=2
casehub.workers.k8s.jobs.report-gen.memory-limit=4Gi

# Template-based job (template takes precedence over image fields)
casehub.workers.k8s.jobs.ml-inference.template=jobs/ml-inference.yaml
casehub.workers.k8s.jobs.ml-inference.namespace=ml
casehub.workers.k8s.jobs.ml-inference.timeout-seconds=7200
casehub.workers.k8s.jobs.ml-inference.max-output-bytes=4194304
```

### Design Decisions

**`backoffLimit` defaults to 0:** CaseHub's fault pipeline owns retry logic. K8s-level retries (`backoffLimit > 0`) create two competing retry mechanisms. With `backoffLimit=0` + `restartPolicy: Never`, any Pod failure surfaces immediately to CaseHub's fault handler. Advanced users can set `backoffLimit > 0` for K8s-level transients (node eviction, kubelet restart).

**`ttlAfterFinished` minimum 300s:** Enforced at config validation time. The TTL is a safety net for cases where eager delete in `processTerminal()` doesn't execute (informer miss, worker pool saturation). Setting it too low creates a race: the K8s TTL controller could delete the Job before `processTerminal()` runs, causing a spurious "Job deleted externally" retryable fault for a successfully completed Job.

**`maxInputBytes` defaults to 256KB:** K8s stores the full Pod spec in etcd, which has a ~1.5MB object size limit. `CASEHUB_INPUT_DATA` is JSON-serialized `inputData` injected as an env var. At dispatch time, if the serialized JSON exceeds `maxInputBytes`, throw `PermanentFaultException(0, "Input data exceeds maxInputBytes limit")`. This prevents cryptic 422 errors from the API server.

**Capability tag format:** `k8s:<name>` (e.g., `k8s:report-gen`). Consistent with `script:<name>` and `mcp:<server>:<tool>`.

**Single-tier resolution (config-only):** No EndpointRegistry integration. The K8s "endpoint" is the cluster itself (via kubeconfig), not a per-capability endpoint. Consistent with Script's config-only model.

**Tenant namespace isolation:** Job namespace is configured per job definition, not per tenant. `tenancyId` from `ProvisionContext` is injected as a label (`casehub.io/tenancy-id`) and env var (`CASEHUB_TENANCY_ID`) but does not influence namespace selection. For multi-tenant deployments requiring hard isolation, operators should configure separate job definitions per tenant with distinct namespaces and K8s ResourceQuotas. The informer's label selector (`app.kubernetes.io/managed-by=casehub`) scopes observation; namespace-level RBAC provides the isolation boundary.

---

## Dispatch and Completion Flow

### Dispatch (`submit()`)

1. Resolve `JobDefinition` from capability tag. Catch `WorkerProvisioningException` separately — build ctx just for the fault, publish fault immediately, return `voidItem()`.
2. Validate input size: serialize `inputData` to JSON, check byte length against `maxInputBytes`. If exceeded → `PermanentFaultException(0, "Input data (" + size + " bytes) exceeds maxInputBytes limit (" + limit + ")")`.
3. Build `WorkerCorrelationContext` (only reached if resolution succeeded).
4. Register in `AsyncWorkerCompletionRegistry`:
   - TTL = `timeoutSeconds + 300` (5-minute buffer for informer latency)
   - `provisionerMeta = Map.of("cleanup", definition.cleanup().name())`
5. Build K8s Job via `K8sJobBuilder`. Enforce `restartPolicy: Never` — warn if template had a different value.
6. Create Job via `kubernetesClient.resource(job).create()`.
7. On create failure: `registry.complete(pending.dispatchId())` to clean up, then classify API error and throw.
8. Outer `onFailure().recoverWithUni()` publishes fault to `K8S_WORKER_FAULT`.
9. Execution model: `runSubscriptionOn(Infrastructure.getDefaultWorkerPool())` — fabric8 client API is blocking.

### `getActiveWorkCount()`

Delegates to `asyncWorkerCompletionRegistry.countByWorkerName(workerId)`. Consistent with HTTP and Camel workers, which also use `AsyncWorkerCompletionRegistry`.

### Completion (informer callback)

Per-namespace informers, one per unique namespace from config. Stored in `Map<String, SharedIndexInformer<Job>>`. Label selector: `app.kubernetes.io/managed-by=casehub`.

**Common preamble (all three handlers):** Read `casehub.io/dispatch-id` label. If null → log at DEBUG and return (Job has the managed-by label but is not a CaseHub-dispatched Job). This guards against `ConcurrentHashMap.remove(null)` → NPE when other tooling in the same namespace uses the `app.kubernetes.io/managed-by=casehub` label.

**`onAdd`:** Check terminal state → hop to worker pool → `processTerminal()`. Same pattern as `onUpdate`. Handles informer reconnection after watch disconnection (410 Gone triggers a full re-list; Jobs completed during the disconnection window appear as terminal `onAdd` events — the only signal for those completions). Non-terminal `onAdd` events are ignored (we created the Job). `processTerminal()` step 1's own `registry.complete()` provides both double-fire protection and filtering of unknown dispatch IDs. This is not restart recovery — it handles the informer's own reconnection semantics within a single JVM session.

**`onUpdate`:** Check terminal state → hop to worker pool → `processTerminal()`.

**`onDelete`:** Read the cached Job object's terminal state (fabric8's `onDelete` receives the last cached version). If the cached Job is terminal (Succeeded or Failed) → route to `processTerminal()` (the deletion is post-completion cleanup by the TTL controller, not an interruption). If the cached Job is NOT terminal → `registry.complete(dispatchId)`: if entry exists → publish fault "Job deleted externally" (genuine mid-execution deletion). If empty → already processed, ignore.

### `processTerminal()`

1. `registry.complete(dispatchId)` — atomic remove. If empty (already processed/expired) → stop.
2. **If succeeded:** capture Pod logs via `K8sJobOutputCapture`, parse output, publish completion via `WorkflowCompletionPublisher`.
3. **If failed:** classify fault (see Fault Classification), publish via `WorkerFaultPublisher` to `K8S_WORKER_FAULT`.
4. Read cleanup policy from `pending.provisionerMeta().get("cleanup")`. If not `RETAIN` → delete Job (best-effort, log failures).

### Sequence Diagram

```
submit()                    K8s API Server              Informer
  │                              │                         │
  ├─register(pendingCompletion)  │                         │
  ├─create(job)─────────────────>│                         │
  ├─return Uni<Void>             │                         │
  │                              │    ... Job runs ...     │
  │                              │──onUpdate(terminal)────>│
  │                              │                         ├─read dispatch-id label
  │                              │                         ├─registry.complete()
  │                              │                         ├─capture Pod logs
  │                              │                         ├─publish completion/fault
  │                              │<───delete(job)──────────┤
```

### Thread Safety

- `registry.complete()` — `ConcurrentHashMap.remove()`, atomic
- `completionPublisher.complete()` — `EventBus.publish()`, thread-safe
- `faultPublisher.fault()` — `EventBus.publish()`, thread-safe
- Pod log reads and Job deletes — stateless HTTP calls, thread-safe
- Double-fire protection: `registry.complete()` returns `Optional` — only one caller gets the entry

---

## Fault Classification

| K8s Failure | Classification | Rationale |
|---|---|---|
| `BackoffLimitExceeded` (backoffLimit=0) | Check Pod reason | Single Pod failure — classify by Pod-level cause |
| `BackoffLimitExceeded` (backoffLimit>0) | **Permanent** | K8s already retried N times |
| `DeadlineExceeded` | **Permanent** | Timeout — enriched with Pod waiting state if Pod never started |
| `OOMKilled` | **Permanent** | Memory limit too low — retrying wastes resources |
| `ImagePullBackOff` / `ErrImagePull` | **Permanent** | Wrong image or missing credentials |
| `InvalidImageName` | **Permanent** | Malformed image reference |
| `CreateContainerConfigError` | **Permanent** | Bad volume mounts, missing ConfigMaps/Secrets |
| Pod eviction (`Evicted`) | **Retryable** | Node memory/disk pressure — transient infrastructure condition |
| Pod preemption (`Preempting`) | **Retryable** | Higher-priority Pod displaced this one — transient |
| Node failure (Pod disappeared) | **Retryable** | With `backoffLimit=0`, Job fails; node failure is transient |
| Generic non-zero exit | **Retryable** | Could be transient |
| Job deleted externally | **Retryable** | Operator intervention, may succeed on retry |
| API server 403 Forbidden | **Permanent** | RBAC misconfiguration |
| API server 404 Not Found | **Permanent** | Namespace doesn't exist |
| API server 422 Unprocessable | **Permanent** | Invalid Job spec |
| API server 409 Conflict | **Retryable** | Name collision |
| Network error | **Retryable** | Transient connectivity |

### DeadlineExceeded Enrichment

When classifying `DeadlineExceeded`, inspect the last Pod's waiting state. If the Pod never started, include the root cause in the error message:

```
DeadlineExceeded — Pod never started: ImagePullBackOff (acme/nonexistent:latest)
```

vs.

```
DeadlineExceeded — Job ran past activeDeadlineSeconds (3600s)
```

---

## Informer Lifecycle and Runtime

### `K8sJobInformerManager`

`@ApplicationScoped`. Manages the shared informer pool.

**Startup (`start(namespaces)`):**
- One `SharedIndexInformer<Job>` per unique namespace
- Label selector: `app.kubernetes.io/managed-by=casehub`
- Per-namespace error isolation: failed namespace → log warning, mark unavailable. Dispatch to failed namespace → `PermanentFaultException`.

**Shutdown (`stop()`):**
- Close all informers, best-effort.

### `K8sWorkerRuntime`

`@ApplicationScoped`, implements `WorkerRuntime`.

```
workerType() → "k8s"
status()     → PENDING | RUNNING | FAULTED | STOPPED

initialize():
  1. resolver.initialize()  — load config
  2. If no jobs configured → FAULTED
  3. Validate K8s connectivity — kubernetesClient.getApiVersion()
  4. Collect unique namespaces from all JobDefinitions
  5. informerManager.start(namespaces)
  6. If all informers failed → FAULTED
  7. Else → RUNNING

shutdown():
  1. informerManager.stop()
  2. → STOPPED

capabilities() → resolver.capabilities()
```

FAULTED → RUNNING recovery: calling `initialize()` retries initialization.

---

## Provisioner and Capability Resolution

### `K8sReactiveWorkerProvisioner`

`@ApplicationScoped`, implements `ReactiveWorkerProvisioner`. Follows `ScriptReactiveWorkerProvisioner` pattern:

- `provision()`: `resolver.firstMatch()` → `ProvisionResult.empty()` or failure
- `terminate()`: no-op (stateless)
- `getCapabilities()`: delegates to resolver

### `JobDefinitionResolver`

`@ApplicationScoped`, implements `WorkerCapabilityResolver<JobDefinition>`. Config-driven, single-tier.

- `initialize()`: loads global defaults + per-job config, validates (image non-blank or template loadable, capability tag `k8s:{name}` ≤ 63 chars for K8s label value compliance), builds `Map<String, JobDefinition>`
- `resolve(tag, tenancyId)`: strips `k8s:` prefix, looks up in map, throws `WorkerProvisioningException` if not found
- `firstMatch(capabilities, tenancyId)`: first `k8s:*` tag that exists in map
- `capabilities()`: immutable set of all `k8s:{name}` tags
- `namespaces()`: unique set of all `JobDefinition` namespaces

### Global Defaults

| Field | Default | Config key |
|---|---|---|
| `namespace` | `"default"` | `casehub.workers.k8s.namespace` |
| `timeoutSeconds` | `3600` | `casehub.workers.k8s.timeout-seconds` |
| `ttlAfterFinished` | `600` | `casehub.workers.k8s.ttl-after-finished` |
| `backoffLimit` | `0` | `casehub.workers.k8s.backoff-limit` |
| `cleanup` | `DELETE` | `casehub.workers.k8s.cleanup` |
| `maxOutputBytes` | `1048576` | `casehub.workers.k8s.max-output-bytes` |
| `maxInputBytes` | `262144` | `casehub.workers.k8s.max-input-bytes` |

---

## Job Builder

### `K8sJobBuilder`

Static utility. Two paths:

**Image-based** (no template): Builds full Job spec from `JobDefinition` fields.

**Template-based** (template set): Loads YAML from classpath, deserializes to fabric8 `Job`, overlays CaseHub fields.

### Invariants (both paths)

- `restartPolicy: Never` — always enforced, warn if template had different value
- Unique name: `casehub-{slug}-{8-char-hex}` (max 57 chars, within K8s 63-char limit)
- Labels: `app.kubernetes.io/managed-by=casehub`, `casehub.io/dispatch-id={id}`, `casehub.io/capability={tag}`, `casehub.io/tenancy-id={tenancyId}`, plus user-defined
- Env vars: `CASEHUB_CASE_ID`, `CASEHUB_TENANCY_ID`, `CASEHUB_CAPABILITY`, `CASEHUB_IDEMPOTENCY`, `CASEHUB_INPUT_DATA` (JSON-serialized inputData, pre-validated against `maxInputBytes`), plus user-defined

### Slug Derivation

1. Strip `k8s:` prefix from the capability tag
2. Lowercase the result
3. Replace characters not matching `[a-z0-9-]` with `-`
4. Collapse consecutive `-` into one; strip leading/trailing `-`
5. Truncate to 40 chars (budget: `57 - len("casehub-") - len("-xxxxxxxx") = 40`)
6. If truncated, replace the last 5 chars with a 5-char hash of the full pre-truncation slug to prevent prefix collisions

### Template Overlay

Overlay order for templates:
1. `metadata.name` → generated unique name
2. `metadata.namespace` → from config
3. `metadata.labels` → merge (CaseHub labels win on collision)
4. `spec.backoffLimit` → from config
5. `spec.activeDeadlineSeconds` → from config
6. `spec.ttlSecondsAfterFinished` → from config
7. `spec.template.spec.restartPolicy` → `Never` (enforced)
8. First container env vars → append CaseHub env vars

---

## Output Capture

### `K8sJobOutputCapture`

`@ApplicationScoped`. Reads Pod logs after Job completion. Accepts `maxOutputBytes` from `JobDefinition` — not from global config.

1. List Pods by `job-name` label in the Job's namespace.
2. Select last Pod (sort by `creationTimestamp` descending) — handles `backoffLimit > 0` case.
3. Read exit code from `pod.status.containerStatuses[0].state.terminated.exitCode`.
4. Read logs bounded at `maxOutputBytes` (via K8s API `limitBytes` parameter or manual truncation).
5. Parse: valid JSON object → structured `Map<String, Object>`; anything else → `{stdout: <raw>, exitCode: <code>}`.
6. On any exception (Pod GC'd, logs unavailable) → return `Map.of()` with warning log.

---

## PermanentFaultException Usage

No `workers-common` change required. Use the existing `PermanentFaultException(int statusCode, String message)` constructor:

- **K8s API errors** (403, 404, 422): `new PermanentFaultException(statusCode, message)` — preserves the actual HTTP status code for test assertions, consistent with HTTP, MCP, and GitHub Actions workers.
- **Pod-level errors** (OOMKilled, ImagePullBackOff, DeadlineExceeded, eviction): `new PermanentFaultException(0, message)` — no HTTP status code applies, consistent with Script worker's `(0, message)` pattern for non-HTTP errors.

---

## Scope Boundaries

**In scope:**
- Single cluster (Quarkus-injected `KubernetesClient`)
- Config-driven job definitions (image-based and template-based)
- Shared informer completion model
- Full fault classification with K8s-specific error mapping
- Pod log output capture
- Eager delete + TTL safety net cleanup

**Out of scope (v1):**
- Multi-cluster dispatch (#16)
- Restart recovery (#17) — in-memory registry lost on restart. The engine's stalled-worker detection re-dispatches, but for long-running K8s Jobs (hours, not seconds) this means: (1) the original Job runs to completion unobserved, (2) its output is lost when `ttlSecondsAfterFinished` expires, (3) a duplicate Job is dispatched, wasting compute proportional to job duration. This is qualitatively worse than HTTP async (where re-dispatch cost is negligible). Issue #17 tracks registry persistence or K8s-side recovery (re-list Jobs with `app.kubernetes.io/managed-by=casehub` labels on startup).
- Namespace validation at init (validated at dispatch time — consistent with MCP tool validation)
- EndpointRegistry integration (cluster is fixed, not per-capability)

---

## RBAC Requirements

The K8s service account used by the worker needs the following minimum permissions per configured namespace:

| Resource | API Group | Verbs |
|---|---|---|
| `jobs` | `batch/v1` | `create`, `delete`, `list`, `watch` |
| `pods` | `v1` | `list`, `get` |
| `pods/log` | `v1` | `get` |

Example `Role` (one per namespace):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: casehub-worker
  namespace: batch
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "delete", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list", "get"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
```

Missing permissions surface as `PermanentFaultException(403, ...)` at dispatch time. The informer also requires `list` + `watch` on `jobs` — a missing `watch` permission causes the informer to fail at startup, setting the runtime to `FAULTED`.

---

## Operational Notes

### CleanupPolicy.RETAIN Memory Impact

When `RETAIN` is configured, completed Jobs stay in K8s indefinitely. The `SharedIndexInformer` maintains a local cache of all matching Jobs (label selector `app.kubernetes.io/managed-by=casehub`). Retained Jobs accumulate in the informer cache, consuming JVM heap proportional to the number of retained Jobs × Pod spec size.

Recommendation: prefer `ttlSecondsAfterFinished` for debugging (keeps the Job for a bounded time) over `RETAIN` (unbounded). Use `RETAIN` only for development environments with manual cleanup.

---

## ARC42STORIES Placement

Implementation adds to `ARC42STORIES.MD`:

- **Layer L9 — K8s Dispatch** in the Layer Taxonomy table
- **Chapter 7 — Kubernetes Job Worker** in §9.3 (following Chapter 6's template)
- Layer × Chapter Matrix: column C7 with L1 Low, L2 Medium, + L9 High
- Chapter Index: row 7
- System Context diagram: add `k8s` system and `K8s API Server` external system
- Flowchart: `C6 --> C7["C7: K8s Worker\n+L9"]`

---

## Platform Documentation Updates

PLATFORM.md capability ownership entry "Worker dispatch" is stale — lists only HTTP, Camel, GitHub Actions, and MCP. Implementation updates this to include all six worker types: Camel, HTTP, GitHub Actions, MCP, Script, K8s.

---

## Protocol Coherence

| Protocol | Status |
|---|---|
| Module tier structure (PP-20260512) | Tier 2 integration module, no JPA — compliant |
| Maven coordinate standard (PP-20260512-coord) | `io.casehub` / `casehub-workers-k8s` / `0.2-SNAPSHOT` — compliant |
| Maven submodule folder naming (PP-20260508) | `workers-k8s` follows existing repo convention (all siblings use `workers-*` prefix) |
| Reactive vs blocking (PP-20260521) | Blocking for Job creation (`runSubscriptionOn`), event-driven for completion (informer) — appropriate for mixed I/O |
| Worker function execution model (PP-20260531) | Not applicable — worker dispatches, doesn't define worker functions |
| Provisioner returns ProvisionResult (PP-20260529) | `ProvisionResult.empty()` — compliant |
| Engine worker event observer (PP-20260531) | Fault handler stub uses `@ConsumeEvent(blocking=true)` — compliant |
