# Design Spec: workers-github-actions — GitHub Actions Workflow Dispatch Worker

**Date:** 2026-06-10
**Issue:** casehubio/casehub-workers#6
**Status:** Approved (rev 2)
**Review cycles:** 2 — 10 issues total, 10 accepted (1 with modification)

---

## 1. Overview

A new `workers-github-actions` module that dispatches CaseHub case steps to GitHub Actions workflows via GitHub's REST API. Two trigger types are supported: `workflow_dispatch` (trigger a specific workflow with structured inputs) and `repository_dispatch` (fire a custom event type with a payload). Fire-and-forget completion model — dispatch succeeds (204), case step completes immediately. No async completion registry, no pending completions.

Follows the established worker module pattern: `ReactiveWorkerProvisioner` + `WorkerExecutionManager` SPIs, CDI activation by classpath presence, worker-specific fault address, `WorkerRetrySupport` for retry infrastructure.

---

## 2. Prerequisite: Extract Fault Exception Types to workers-common

`PermanentFaultException` and `RetryAfterException` currently live in `io.casehub.workers.http`. These are not HTTP-specific concepts — "don't retry" and "retry after this delay" apply to any worker that talks to a fallible external API. The Camel worker would benefit from `PermanentFaultException` for routes that signal permanent failure.

**Extraction scope:**

| Type | From | To | Rationale |
|------|------|----|-----------|
| `PermanentFaultException` | `io.casehub.workers.http` | `io.casehub.workers.common` | Worker-agnostic "don't retry" signal |
| `RetryAfterException` | `io.casehub.workers.http` | `io.casehub.workers.common` | Worker-agnostic "retry after delay" signal |
| `parseRetryAfter()` | `HttpWorkerExecutionManager` (instance method) | `WorkerRetrySupport` (static method) | GitHub's API also returns `Retry-After` on 429 — shared utility |

The HTTP worker becomes a consumer, not the owner. Import paths change; `HttpWorkerFaultEventHandler` and `HttpWorkerExecutionManager` update their imports. Mechanical — same pattern as the `WorkerRetrySupport` extraction from the Camel module in the HTTP spec.

This is a prerequisite step, completed before the GitHub Actions module is created.

---

## 3. Capability Tags and InputData Contract

### 3.1 Capability tags

Two static capability tags — no endpoint resolver, no dynamic discovery:

- `github-actions:workflow-dispatch` — `POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches`
- `github-actions:repository-dispatch` — `POST /repos/{owner}/{repo}/dispatches`

Tags are simple logical names. Routing information (owner, repo, workflow) is carried in `inputData`, not encoded in the tag. GitHub's deterministic URL structure makes an endpoint resolver unnecessary.

### 3.2 InputData contract

**`workflow-dispatch`:**

```json
{
  "owner": "casehubio",
  "repo": "devtown",
  "workflow_id": "ci.yml",
  "ref": "main",
  "inputs": { "environment": "staging" }
}
```

- `owner`, `repo`, `workflow_id`, `ref` — **all required**. Missing any → `PermanentFaultException`. The GitHub API requires `ref` — omitting it returns 422.
- `inputs` — optional, omitted from request body when null/empty.

**`repository-dispatch`:**

```json
{
  "owner": "casehubio",
  "repo": "devtown",
  "event_type": "upstream-published",
  "client_payload": { "source": "casehub-engine" }
}
```

- `owner`, `repo`, `event_type` — required. Missing → `PermanentFaultException`.
- `client_payload` — optional, omitted from request body when null/empty.

---

## 4. Module Structure

### 4.1 Coordinates

- **Module directory:** `workers-github-actions`
- **Artifact:** `casehub-workers-github-actions`
- **Root package:** `io.casehub.workers.githubactions`
- **workerType:** `"github-actions"`

### 4.2 Class inventory

| Class | Responsibility |
|-------|---------------|
| `GitHubActionsWorkerConstants` | `WORKER_TYPE = "github-actions"` |
| `GitHubActionsWorkerEventBusAddresses` | `GITHUB_ACTIONS_WORKER_FAULT = "casehub.workers.github-actions.fault"` |
| `GitHubActionsWorkerFaultPublisher` | Publishes `WorkflowExecutionFailed` to the fault address |
| `GitHubActionsWorkerFaultEventHandler` | `@ConsumeEvent(GITHUB_ACTIONS_WORKER_FAULT, blocking=true)` — retry logic via `WorkerRetrySupport` |
| `GitHubActionsReactiveWorkerProvisioner` | Validates capability tags, checks token resolvable |
| `GitHubActionsWorkerExecutionManager` | Dispatches via Vert.x WebClient, completes immediately on 204 |
| `GitHubActionsTokenResolver` | Resolves PAT from config — global default + optional per-org override |

No `*EndpointResolver` — deterministic URL structure makes it unnecessary.

No `*CompletionExpiryObserver`, no `*FaultCallbackObserver` — fire-and-forget model does not register pending completions. No pending completions means no expiry events and no callback events to observe. These observers are trivial (~15 lines each) and will be added when the completion model evolves to polling or webhook-based confirmation.

No `RetryableDispatchException` — 422 on `workflow-dispatch` uses `RetryAfterException(60_000)` (see §7.2). 422 on `repository-dispatch` is `PermanentFaultException` (malformed request).

### 4.3 Dependencies

```xml
<dependency><groupId>io.casehub</groupId><artifactId>casehub-workers-common</artifactId></dependency>
<dependency><groupId>io.casehub</groupId><artifactId>casehub-engine-api</artifactId></dependency>
<dependency><groupId>io.casehub</groupId><artifactId>casehub-engine-common</artifactId></dependency>
<dependency><groupId>io.quarkus</groupId><artifactId>quarkus-arc</artifactId></dependency>
<dependency><groupId>io.quarkus</groupId><artifactId>quarkus-vertx</artifactId></dependency>
<dependency><groupId>io.smallrye.reactive</groupId><artifactId>smallrye-mutiny-vertx-web-client</artifactId></dependency>
```

Test scope: `casehub-workers-testing`, `quarkus-junit`, `assertj-core`, `mockito-core`.

**`casehub-platform-api` is not listed.** Zero imports of `io.casehub.platform` exist in any Java file across the entire workers repo (workers-common, workers-http, workers-camel). The dependency is cargo-culted in the existing modules. workers-common provides it transitively if ever needed. The broader cleanup (removing it from existing modules) is a separate task.

---

## 5. Auth Model

### 5.1 Config properties

```properties
# Global default — used when no org-specific override matches
casehub.workers.github-actions.token=ghp_xxxxxxxxxxxxx

# Optional per-org override — matches the "owner" field in inputData
casehub.workers.github-actions.tokens.casehubio=ghp_yyyyyyyyyy
casehub.workers.github-actions.tokens.other-org=ghp_zzzzzzzzzz

# GitHub API base URL — override for GitHub Enterprise
# Default: https://api.github.com
casehub.workers.github-actions.api-base-url=https://github.example.com/api/v3
```

### 5.2 Resolution order (GitHubActionsTokenResolver)

1. Per-org: `casehub.workers.github-actions.tokens.{owner}` — if present, use it.
2. Global: `casehub.workers.github-actions.token` — fallback.
3. Neither → `PermanentFaultException`. Misconfiguration — no retry.

This is Tier 1 (static deploy-time config), consistent with the outbound auth policy in PLATFORM.md. Tier 2 via `EndpointRegistry` (platform#73) is future work — the module works without it.

### 5.3 Token type requirement

Cross-repo `repository_dispatch` requires a classic PAT with `repo` scope. Fine-grained PATs lack org-level dispatch support (GE-20260501-d9c2d7). The token resolver doesn't enforce token type — it resolves whatever string is configured — but this requirement should be documented.

---

## 6. Dispatch and Completion Flow

### 6.1 Execution manager submit() flow

1. **Validate inputData** — required fields by capability tag. `workflow-dispatch` requires `owner`, `repo`, `workflow_id`, `ref`. `repository-dispatch` requires `owner`, `repo`, `event_type`. Missing any → `PermanentFaultException`.

2. **Resolve auth token** — `GitHubActionsTokenResolver.resolve(owner)`.

3. **Construct URL:**
   - `workflow-dispatch`: `{apiBaseUrl}/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches`
   - `repository-dispatch`: `{apiBaseUrl}/repos/{owner}/{repo}/dispatches`

4. **Build request body:**
   - `workflow-dispatch`: `{"ref": "{ref}", "inputs": {inputs}}` — `inputs` omitted when null/empty
   - `repository-dispatch`: `{"event_type": "{event_type}", "client_payload": {client_payload}}` — `client_payload` omitted when null/empty

5. **POST via Vert.x WebClient** with headers:
   - `Authorization: Bearer {token}`
   - `Accept: application/vnd.github+json`
   - `X-GitHub-Api-Version: 2022-11-28`

   No CaseHub headers (`X-CaseHub-*`) are sent. GitHub's API ignores unknown headers — they serve no observability or correlation purpose. The HTTP and Camel workers send CaseHub headers because their receivers are CaseHub-aware services. GitHub is not. If CaseHub correlation data needs to reach the workflow, the case definition author should include it in `inputs` or `client_payload`.

6. **Handle response:**
   - **204** → publish `WorkflowExecutionCompleted` via `WorkflowCompletionPublisher`. Output: `Map.of("dispatched", true, "owner", owner, "repo", repo)`.
   - **422 on `workflow-dispatch`** → `RetryAfterException(60_000)`. The `workflow_dispatch` trigger definition is cached by GitHub (GE-20260426-805acb) — refreshes take "several minutes to tens of minutes." A 60-second retry delay gives the cache time to refresh. The default policy (3 attempts, 10s FIXED) would exhaust within 30 seconds, before the cache refreshes.
   - **422 on `repository-dispatch`** → `PermanentFaultException`. Unlike `workflow-dispatch`, a 422 here means a genuinely malformed request (bad `event_type`, invalid payload) — retrying is pointless.
   - **429** → parse `Retry-After` header via `WorkerRetrySupport.parseRetryAfter()`, throw `RetryAfterException`.
   - **401/403** → `PermanentFaultException`. Bad token.
   - **404** → `PermanentFaultException`. Repo or workflow doesn't exist.
   - **Other 4xx** → `PermanentFaultException`.
   - **5xx** → `RuntimeException` → retryable via standard fault handler.

7. **All failures** route through `GitHubActionsWorkerFaultPublisher` → fault event handler → `WorkerRetrySupport`.

No `emitOn` needed — WebClient is event-loop native, same as workers-http.

No pending completion registration — fire-and-forget completes synchronously on 204. There is no "waiting" state between dispatch and completion where a pending completion adds value. If the POST fails, the fault publisher handles it immediately. Adding a pending completion would create a timing window where the expiry timer could fire before the 204 arrives on a slow response, routing a successful dispatch into the fault pipeline.

### 6.2 Other WorkerExecutionManager SPI methods

- **`schedulePersistedEvent(EventLog)`** — returns `Uni.createFrom().voidItem()`. No Quartz persisted events.
- **`getActiveWorkCount(String workerId)`** — returns `0`. No pending completions are tracked under the fire-and-forget model.
- **`getActiveCaseIds(String workerId)`** — inherits default `List.of()`.

---

## 7. Fault Handling and Retry

### 7.1 Fault event handler

Follows `HttpWorkerFaultEventHandler` structure:

1. **Always persist** — `retrySupport.persistFailureLog()` for observability.

2. **Permanent fault check** — `PermanentFaultException` (401, 403, 404, other 4xx, 422 on `repository-dispatch`) → skip retry → `retrySupport.publishRetriesExhausted()`.

3. **Count and check** — `retrySupport.countFailedAttempts()` against `retryPolicy.maxAttempts()` (strict `<`). Null policy defaults to `new RetryPolicy()` (3 attempts, 10s FIXED).

4. **Compute delay:**
   - `RetryAfterException` (429 or 422 on `workflow-dispatch`) → use `retryAfterMs`.
   - Other retryable → `WorkerRetrySupport.computeBackoffDelayMs()`.

5. **Re-dispatch** — reload `EventLog`, rebuild inputData, schedule via `Vertx.setTimer(delayMs, ...)`, re-submit. No `emitOn` — WebClient is event-loop native.

6. **Retries exhausted** → `retrySupport.publishRetriesExhausted()`.

7. **Recovery** — `.onFailure().recoverWithUni()` logs and swallows so fault-handling bugs don't crash the event bus consumer.

### 7.2 422 handling — workflow-dispatch vs repository-dispatch

The 422 status code means different things for the two trigger types:

- **`workflow-dispatch` 422** — likely the trigger definition cache (GE-20260426-805acb). The workflow_dispatch trigger was recently added to the workflow file, but GitHub's cached workflow definition hasn't refreshed yet. Retrying after 60 seconds gives the cache time to update. Expressed as `RetryAfterException(60_000)` — the fault handler already knows how to handle this type (uses `retryAfterMs` as the timer delay instead of configured backoff).

- **`repository-dispatch` 422** — malformed request. The `event_type` or payload doesn't match what GitHub expects. Retrying with the same inputData (replayed from `EventLog`) will fail identically. Expressed as `PermanentFaultException`.

---

## 8. Provisioner

### 8.1 getCapabilities()

Returns `Set.of("github-actions:workflow-dispatch", "github-actions:repository-dispatch")`. Static — always available when the module is on the classpath.

### 8.2 provision()

Checks that at least one requested capability matches the supported set. Validates that a token is resolvable (calls `GitHubActionsTokenResolver` with no specific org — checks global default exists). Returns `ProvisionResult.empty()`. No external API call.

**Limitation:** Provisioning validates capability tag and global token availability only. Per-org token resolution is deferred to dispatch time — `ProvisionContext` does not carry `inputData`, so the provisioner cannot see the `owner` field. A missing per-org token with no global fallback is a dispatch-time fault (`PermanentFaultException`), not a provisioning rejection. This matches the HTTP provisioner pattern (validates endpoint registration, not reachability).

### 8.3 terminate()

Returns `Uni.createFrom().voidItem()`. Nothing to tear down.

### 8.4 Activation

`@ApplicationScoped` — displaces `NoOpReactiveWorkerProvisioner` by CDI priority when on classpath.

---

## 9. Testing Strategy

All tests are pure unit tests — Mockito + AssertJ, no `@QuarkusTest`, no network.

### 9.1 Prerequisite extraction (workers-common)

- `PermanentFaultException` and `RetryAfterException` moved — existing HTTP worker tests still pass with updated imports.
- `WorkerRetrySupport.parseRetryAfter()` — integer seconds, HTTP-date in future, HTTP-date in past, unparseable, null (existing tests migrated from `HttpWorkerExecutionManagerTest`).

### 9.2 Token resolver

- Global fallback resolves correctly.
- Per-org override takes precedence over global.
- Missing token (no global, no org match) throws `PermanentFaultException`.

### 9.3 Execution manager

- 204 → `WorkflowExecutionCompleted` published, correct URL constructed for each trigger type.
- Missing required inputData fields (`owner`, `repo`, `workflow_id`, `ref` for workflow-dispatch; `owner`, `repo`, `event_type` for repository-dispatch) → `PermanentFaultException`.
- 422 on `workflow-dispatch` → `RetryAfterException(60_000)`.
- 422 on `repository-dispatch` → `PermanentFaultException`.
- 429 with `Retry-After` → `RetryAfterException` with correct delay (via shared `parseRetryAfter()`).
- 401/403/404 → `PermanentFaultException`.
- 5xx → plain `RuntimeException` (retryable).
- Custom `api-base-url` → URL uses configured base.
- Request body: correct JSON for each trigger type, `inputs`/`client_payload` omitted when null/empty.
- No CaseHub headers on outbound request.
- `schedulePersistedEvent` returns void item.
- `getActiveWorkCount` returns 0.

### 9.4 Fault event handler

- Permanent fault → skips retry, publishes exhausted.
- Retryable fault within max attempts → re-dispatches after delay.
- Retries exhausted → publishes exhausted.
- `RetryAfterException` (422 workflow-dispatch or 429) → uses `retryAfterMs` as delay.

### 9.5 Provisioner

- Matching capability → `ProvisionResult.empty()`.
- Unmatched capability → throws.
- Global token missing → provisioning fails.

---

## 10. Co-deployment Constraints

Same constraint as other worker modules: CDI ambiguity on `WorkerExecutionManager` when co-deployed with workers-camel, workers-http, or scheduler-quartz. Blocked by engine#461 (composite `WorkerExecutionManager`).

---

## 11. Garden References

- **GE-20260501-d9c2d7** — GITHUB_TOKEN returns 403 on cross-repo `repository_dispatch` — needs classic PAT. Informs auth model (§5.3).
- **GE-20260501-c579bb** — Chain CI/CD across repos using `repository_dispatch`. Validates the `repository-dispatch` capability tag design. **Note:** the constraints section of this entry has a factual error — claims GITHUB_TOKEN works for same-org cross-repo dispatch, contradicted by GE-20260501-d9c2d7. Correction to be submitted via forage REVISE.
- **GE-20260426-805acb** — `workflow_dispatch` trigger definition is cached. Informs 422 retryable handling with 60-second delay (§6.1 step 6, §7.2).

---

## 12. Future Evolution

- **Completion model** — fire-and-forget is the current model. Future options: polling GitHub's runs API for completion status, or webhook-based callback from within the workflow. When the model evolves, add `GitHubActionsCompletionExpiryObserver`, `GitHubActionsFaultCallbackObserver`, and pending completion registration in the execution manager. These are trivial (~15 lines each) and follow the pattern established by the HTTP and Camel workers.
- **Tier 2 auth** — `EndpointRegistry` (platform#73) could provide per-capability credential resolution. The token resolver is designed to be extended, not replaced.
- **GitHub App tokens** — installation tokens from GitHub Apps are shorter-lived and more granular than PATs. A future `GitHubAppTokenProvider` could slot into the token resolver without changing the execution manager.
