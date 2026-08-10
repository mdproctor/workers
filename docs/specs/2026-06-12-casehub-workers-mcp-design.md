# Design Spec: workers-mcp — MCP Tool Dispatch Worker

**Date:** 2026-06-12
**Status:** Approved (rev 2)
**Review cycles:** 5 — 24 issues total, 23 accepted, 1 accepted with modification

---

## 1. Overview

A new `workers-mcp` module that dispatches CaseHub case steps to MCP (Model Context Protocol) server tools via Streamable HTTP transport. Case definitions reference MCP tools as capabilities (`mcp:<server>:<tool>`), and the worker translates each dispatch into a JSON-RPC `tools/call` request against a configured MCP server. Request-response completion model — the worker holds the connection (non-blocking Uni) until the tool result arrives, then fires `WorkflowExecutionCompleted`. No async completion registry, no pending completions.

The value proposition is integration breadth: the MCP ecosystem has hundreds of server implementations for different services. Instead of building a dedicated CaseHub worker for each integration, this single worker module can reach any service that exposes an MCP server — Slack, Jira, databases, cloud APIs, internal tools.

Follows the established worker module pattern: `ReactiveWorkerProvisioner` + `WorkerExecutionManager` SPIs, CDI activation by classpath presence, worker-specific fault address, `WorkerRetrySupport` for retry infrastructure.

### 1.1 MCP protocol scope

- **Supported protocol version:** `2025-06-18` (Streamable HTTP transport)
- **Older servers** using the deprecated HTTP+SSE transport (`2024-11-05`) are not supported
- **Backwards compatibility dance** (POST initialize → on 4xx, fall back to GET for SSE endpoint event) is not implemented
- Operators must ensure configured MCP servers support protocol version `2025-06-18`

---

## 2. Capability Tags and InputData Contract

### 2.1 Capability tags

Capabilities are config-driven — operators declare MCP servers and their exposed tools in `application.properties`. Each tool becomes a capability tag in the format `mcp:<server-name>:<tool-name>`:

- `mcp:slack:send-message`
- `mcp:jira:create-issue`
- `mcp:db:query`

The `mcp:` prefix follows the established `<worker-type>:` pattern (`github-actions:workflow-dispatch`, `github-actions:repository-dispatch`). Server name and tool name come from config (see §3).

No dynamic discovery in v1. The worker does not call `tools/list` at startup. Filed as #7 for future work.

### 2.2 InputData contract

InputData is passed through directly as the MCP tool's `arguments` field. No reserved keys, no transformation. The case definition author must know the tool's expected input schema.

Example — dispatching `mcp:slack:send-message`:

```json
{
  "channel": "#general",
  "text": "Build completed successfully"
}
```

This becomes the `tools/call` params:

```json
{
  "name": "send-message",
  "arguments": {
    "channel": "#general",
    "text": "Build completed successfully"
  }
}
```

---

## 3. Configuration

### 3.1 Config properties

```properties
# Server declarations — one block per MCP server
casehub.workers.mcp.servers.slack.url=https://slack-mcp.internal/mcp
casehub.workers.mcp.servers.slack.tools=send-message,list-channels
casehub.workers.mcp.servers.slack.timeout-seconds=30
casehub.workers.mcp.servers.slack.headers.Authorization=Bearer xxx

casehub.workers.mcp.servers.jira.url=https://jira-mcp.internal/mcp
casehub.workers.mcp.servers.jira.tools=create-issue,search-issues
casehub.workers.mcp.servers.jira.timeout-seconds=60
casehub.workers.mcp.servers.jira.headers.X-Api-Key=abc123

# No-auth internal service
casehub.workers.mcp.servers.db.url=https://db-mcp.internal/mcp
casehub.workers.mcp.servers.db.tools=query,insert

# Global default timeout
casehub.workers.mcp.default-timeout-seconds=30
```

### 3.2 Tools property parsing

The `tools` property is a single comma-separated string. Parsing contract:

- Split on `,`
- Trim whitespace from each entry
- Ignore empty strings (trailing comma, double comma)
- Reject duplicate tool names within a server at startup (`WorkerProvisioningException`)
- Missing or empty `tools` property → no capabilities registered for that server (server is configured but exposes nothing)

### 3.3 Server registry — McpServerResolver

`McpServerResolver` implements `WorkerCapabilityResolver<ResolvedMcpServer>`. At startup, it parses all `casehub.workers.mcp.servers.*` config properties and builds a two-level internal structure:

- `Map<String, ResolvedMcpServer>` keyed by server name — shared server config
- `Map<String, String>` mapping each capability tag (`mcp:slack:send-message`) → server name (`slack`)

For the config above, the capability set is: `mcp:slack:send-message`, `mcp:slack:list-channels`, `mcp:jira:create-issue`, `mcp:jira:search-issues`, `mcp:db:query`, `mcp:db:insert`.

`resolve(capabilityTag)` looks up the server name from the tag mapping, then returns the `ResolvedMcpServer`. Multiple capability tags return the same `ResolvedMcpServer` instance — the N:1 mapping is intentional (server config is shared across all its tools). The execution manager parses the tool name from the capability tag at dispatch time.

Throws `WorkerProvisioningException` if the tag is not in the capability set.

Validates at startup: each server must have a non-blank `url` property. Missing URL → `WorkerProvisioningException` at startup, not at dispatch time.

### 3.4 Auth model

Per-server headers in config — same pattern as HTTP endpoints. Arbitrary headers per server cover Bearer tokens, API keys, and custom auth schemes equally. If a server needs no auth, headers are simply not configured. Auth headers are sent on every request to that server, including `initialize` and `initialized`.

No dedicated token resolver — MCP servers don't have a natural grouping hierarchy.

---

## 4. Module Structure

### 4.1 Coordinates

- **Module directory:** `workers-mcp`
- **Artifact:** `casehub-workers-mcp`
- **Root package:** `io.casehub.workers.mcp`
- **workerType:** `"mcp"`

### 4.2 Class inventory

| Class | Responsibility |
|-------|---------------|
| `McpWorkerConstants` | `WORKER_TYPE = "mcp"` |
| `McpWorkerEventBusAddresses` | `MCP_WORKER_FAULT = "casehub.workers.mcp.fault"` |
| `McpWorkerFaultPublisher` | Publishes `WorkflowExecutionFailed` to the fault address |
| `McpWorkerFaultEventHandler` | `@ConsumeEvent(MCP_WORKER_FAULT, blocking=true)` — retry logic via `WorkerRetrySupport` |
| `McpReactiveWorkerProvisioner` | Validates capability tag in resolved set and server URL non-blank |
| `McpWorkerExecutionManager` | Dispatches `tools/call` via Vert.x WebClient, completes on result |
| `McpServerResolver` | Config → `ResolvedMcpServer` (config-only, N:1 tag mapping) |
| `McpSessionManager` | `@ApplicationScoped` — MCP session lifecycle: lazy init, cache, invalidate on 404, cleanup on shutdown |

| Record | Fields |
|--------|--------|
| `ResolvedMcpServer` | `String name, String url, int timeoutSeconds, Map<String, String> headers, Set<String> tools` |

| Mutable class | Purpose |
|---------------|---------|
| `McpSession` | Per-server runtime state — `sessionId`, `protocolVersion`, `AtomicLong requestIdCounter` (starts at 1, spans initialization + tool calls) |

No CDI event observers — no async completion registry means no `CompletionExpiredEvent` or `FaultCallbackEvent` to observe.

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

---

## 5. MCP Session Lifecycle

MCP requires a three-step initialization before any `tools/call` is legal:

1. Client sends `initialize` request (with `protocolVersion`, `capabilities`, `clientInfo`)
2. Server responds with `InitializeResult` (capabilities, serverInfo, optionally `Mcp-Session-Id` header)
3. Client sends `initialized` notification (server responds 202)

Only after this handshake can the client send `tools/call`. The worker manages this via `McpSessionManager`.

Initialization requests use the server's configured `timeout-seconds` (or the global default). No separate initialization timeout — the server timeout is conservative enough for the handshake and avoids an additional config property.

### 5.1 Initialization strategy: lazy with caching

Sessions are established on first dispatch per server, cached, and reused for subsequent dispatches. Re-initialization happens automatically on session expiry (HTTP 404).

- **First dispatch to server `slack`** → `McpSessionManager.getOrInitialize("slack")` triggers the 3-step handshake, caches the resulting `McpSession`, returns it
- **Subsequent dispatches to `slack`** → return cached session immediately, no initialization
- **HTTP 404 on any request** → session expired. `McpSessionManager.invalidate("slack")`. The dispatch faults, and on retry the session manager re-initializes

### 5.2 Concurrent initialization deduplication

Two case steps may dispatch to the same server simultaneously, both triggering initialization. The session manager must ensure only one initialization occurs.

Pattern: `ConcurrentHashMap<String, Uni<McpSession>>` where each `Uni` is memoized:

```java
Uni<McpSession> sessionUni = sessions.computeIfAbsent(serverName,
    k -> performInitialization(k)
        .onFailure().invoke(() -> sessions.remove(k))
        .memoize().indefinitely());
```

- `computeIfAbsent` is atomic for the map operation — only one `Uni` is created per server
- `memoize().indefinitely()` ensures the initialization HTTP calls only execute once; all concurrent subscribers await the same result
- `onFailure().invoke(() -> sessions.remove(k))` — failed initialization removes the cached `Uni` so the next dispatch retries initialization fresh. Failures are not memoized.

No `synchronized` blocks, no blocking I/O on the event loop. The `Uni` subscription is non-blocking — Vert.x event loop safe.

### 5.3 Initialize request

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": {},
    "clientInfo": {
      "name": "CaseHub",
      "version": "0.2"
    }
  }
}
```

- **`protocolVersion`**: `2025-06-18` — the only version this worker supports
- **`capabilities`**: empty `{}` — the worker is a minimal client. It calls tools. It does not support `roots`, `sampling`, or `elicitation`
- **`clientInfo.name`**: `"CaseHub"`. **`clientInfo.version`**: sourced from build metadata (`Implementation-Version` manifest entry) or hardcoded `"0.2"`

### 5.4 Protocol version negotiation

The server responds with a `protocolVersion` in its `InitializeResult`:

- **Server responds with `2025-06-18`** → success. Store in `McpSession.protocolVersion`.
- **Server responds with any other version** → `PermanentFaultException`. The worker does not understand this protocol version. The MCP spec says the client "SHOULD disconnect" on version mismatch.

### 5.5 Session state — Mcp-Session-Id

The server MAY assign a session ID via the `Mcp-Session-Id` response header on the `InitializeResult`. If present:

- Store in `McpSession.sessionId`
- Include as `Mcp-Session-Id` request header on all subsequent requests to that server (including the `initialized` notification and all `tools/call` requests)
- If absent, no session header is sent — the server is stateless

When the server responds with HTTP 404 to any request carrying a `Mcp-Session-Id`, the session has expired. The worker invalidates the cached session and the fault pipeline retries the dispatch (which triggers re-initialization).

### 5.6 Initialization failure handling

Initialization failures flow through the same fault pipeline as tool call failures:

| Failure | Exception | Retryable? |
|---------|-----------|------------|
| Server unreachable (connection error) | `RuntimeException` | Yes |
| HTTP 5xx on `initialize` | `RuntimeException` | Yes |
| HTTP 4xx on `initialize` | `PermanentFaultException` | No |
| JSON-RPC error on `initialize` | `PermanentFaultException` | No |
| Protocol version mismatch | `PermanentFaultException` | No |
| `initialized` notification rejected (non-202) | `RuntimeException` | Yes |

No permanent failure caching in the session manager. If initialization fails permanently (e.g., version mismatch), each dispatch attempt re-tries initialization, gets the same `PermanentFaultException`, and the retry pipeline handles it. This avoids stale failure state — if the server is upgraded between dispatches, the next attempt succeeds.

### 5.7 Session cleanup on shutdown

`McpSessionManager` observes `@PreDestroy`. For each active session with a `sessionId`:

- HTTP DELETE to the server's MCP endpoint with `Mcp-Session-Id` header

Fire-and-forget — no awaiting responses. The MCP spec permits servers to respond with 405 (method not allowed), so cleanup is best-effort. Skipping it means servers accumulate orphaned sessions until they timeout server-side.

---

## 6. Dispatch and Completion Flow

### 6.1 Execution manager submit() flow

1. **Parse capability tag** — `mcp:slack:send-message` → server name `slack`, tool name `send-message`.

2. **Resolve server** — `McpServerResolver.resolve(capabilityTag)` → `ResolvedMcpServer`. If unresolvable, fault immediately with `PermanentFaultException`.

3. **Obtain session** — `McpSessionManager.getOrInitialize(serverName)` → `Uni<McpSession>`. If initialization fails, fault is routed through the standard pipeline (see §5.6).

4. **Build JSON-RPC request body:**
   ```json
   {
     "jsonrpc": "2.0",
     "method": "tools/call",
     "id": 42,
     "params": {
       "name": "<tool-name>",
       "arguments": { ... inputData ... }
     }
   }
   ```
   The JSON-RPC `id` is a per-session `AtomicLong` counter — simple, no edge cases. The idempotency hash from `WorkerExecutionKeys.inputDataHash()` is logged alongside the request for correlation but is not used as the JSON-RPC `id`.

5. **POST to server URL** via Vert.x WebClient with:
   - `Content-Type: application/json`
   - `Accept: application/json, text/event-stream` — **required** by MCP Streamable HTTP transport
   - `MCP-Protocol-Version: 2025-06-18` (or the negotiated version from `McpSession.protocolVersion`) — **required** on all requests after initialization
   - `Mcp-Session-Id: <session-id>` — included when the server assigned one during initialization
   - Configured per-server headers (auth, API keys) — sent after protocol headers
   - Timeout from server config (falls back to global default)

6. **Handle HTTP response** — classify by status code before parsing the body:
   - **2xx** → check `Content-Type` and parse response (see §6.2)
   - **404 with active `Mcp-Session-Id`** → session expired. `McpSessionManager.invalidate(serverName)`. Throw `RuntimeException` (retryable — retry will re-initialize)
   - **404 without `Mcp-Session-Id`** → the MCP endpoint does not exist. `PermanentFaultException`.
   - **429** → parse `Retry-After` header via `WorkerRetrySupport.parseRetryAfter()`, throw `RetryAfterException`
   - **4xx** (except 429, 404) → `PermanentFaultException`
   - **5xx** → `RuntimeException` (retryable)

7. **On timeout** — POST a cancellation notification to the server, fire-and-forget (don't await the 202). Then throw `RuntimeException` (retryable) into the fault pipeline. The cancellation notification is a JSON-RPC notification (no `id` field of its own):
   ```json
   {
     "jsonrpc": "2.0",
     "method": "notifications/cancelled",
     "params": {
       "requestId": 42,
       "reason": "Timeout"
     }
   }
   ```
   `params.requestId` references the `id` of the timed-out `tools/call` request (the `AtomicLong` counter value from step 4). Include `Mcp-Session-Id` and `MCP-Protocol-Version` headers if the session is active.

8. **All failures** route through `McpWorkerFaultPublisher` via `.onFailure().recoverWithUni()`.

No CaseHub headers (`X-CaseHub-*`) — MCP servers follow the MCP protocol, not CaseHub conventions.

No `emitOn` needed — WebClient is event-loop native, same as HTTP and GitHub Actions workers.

### 6.2 Response parsing — dual content type handling

The MCP spec requires the client to support both `application/json` and `text/event-stream` responses. The server chooses which to send.

**`Content-Type: application/json`** — parse body directly as a JSON-RPC response object. This is the simple case.

**`Content-Type: text/event-stream`** — the response body contains SSE-formatted events. The worker buffers the full response body and parses it:

1. Split body on double-newline boundaries into SSE events
2. For each event, extract `data:` lines (concatenated if multi-line)
3. Parse each `data:` payload as JSON. Look for a JSON-RPC response (object with `"result"` or `"error"` key alongside `"id"` matching the request)
4. Ignore all other events (progress notifications, server-initiated requests)
5. If a matching JSON-RPC response is found → process it per §6.3
6. If the stream completes without a matching response → `RuntimeException` (retryable — the server closed the stream prematurely)

**Limitation:** The worker buffers the entire SSE response body. This relies on the server closing the SSE stream after sending the JSON-RPC response (the MCP spec says it SHOULD). If the server keeps the stream open indefinitely, the configured timeout fires and the dispatch faults normally. Progress notifications within the SSE stream do not reset the timeout clock in v1.

### 6.3 JSON-RPC response handling

A parsed JSON-RPC response is one of two shapes:

**JSON-RPC result (success):**

- `isError: false` (or absent) → extract completion output. If `structuredContent` is present, use it directly as the completion output (`Map<String, Object>`). Otherwise, extract the `content` array (deserialized as `List<Map<String, Object>>`) into completion output under a `content` key. Fire `completionPublisher.complete(ctx, output)`.
- `isError: true` → the tool executed but reported a failure. `RuntimeException` (retryable). The MCP spec's own example of `isError: true` is "API rate limit exceeded" — a transient error. The `isError` flag says nothing about permanence. If retries exhaust on a genuinely permanent tool error, the cost is a few wasted attempts. If retries are skipped on a transient tool error, the case stalls permanently.

**JSON-RPC error (protocol-level failure):**

| Code | Meaning | Exception |
|------|---------|-----------|
| `-32600` | Invalid request | `PermanentFaultException` |
| `-32601` | Method not found | `PermanentFaultException` |
| `-32602` | Invalid params | `PermanentFaultException` |
| `-32603` | Internal error | `RuntimeException` (retryable) |
| `-32700` | Parse error | `PermanentFaultException` |
| Other | Server-defined | `RuntimeException` (retryable) |

**Malformed response** (not valid JSON, not valid JSON-RPC, or not valid SSE) → `RuntimeException` (retryable). A malformed response could indicate transient infrastructure issues — a load balancer returning an HTML error page, a proxy timeout, a container restart serving a partial response. If the server is genuinely broken, retries exhaust normally per `RetryPolicy`.

### 6.4 Other WorkerExecutionManager SPI methods

- **`schedulePersistedEvent(EventLog)`** — returns `Uni.createFrom().voidItem()`. No persisted event scheduling.
- **`getActiveWorkCount(String workerId)`** — returns `0`. No pending completions tracked (request-response model).

---

## 7. Fault Handling and Retry

### 7.1 Fault event handler

Follows the established pattern (identical structure to `GitHubActionsWorkerFaultEventHandler`):

1. **Always persist** — `retrySupport.persistFailureLog()` for observability.

2. **Permanent fault check** — `PermanentFaultException` → skip retry → `retrySupport.publishRetriesExhausted()`.

3. **Count and check** — `retrySupport.countFailedAttempts()` against `retryPolicy.maxAttempts()` (strict `<`). Null policy defaults to `new RetryPolicy()` (3 attempts, 10s FIXED).

4. **Compute delay:**
   - `RetryAfterException` (429) → use `retryAfterMs`.
   - Other retryable → `WorkerRetrySupport.computeBackoffDelayMs()`.

5. **Re-dispatch** — reload `EventLog`, rebuild inputData, schedule via `Vertx.setTimer(delayMs, ...)`, re-submit. No `emitOn` — WebClient is event-loop native.

6. **Retries exhausted** → `retrySupport.publishRetriesExhausted()`.

7. **Recovery** — `.onFailure().recoverWithUni()` logs and swallows so fault-handling bugs don't crash the event bus consumer.

### 7.2 Fault classification summary

| Condition | Exception | Retryable? |
|-----------|-----------|------------|
| HTTP 4xx (except 429, 404) | `PermanentFaultException` | No |
| HTTP 404 with active session | `RuntimeException` | Yes (re-initializes) |
| HTTP 404 without session | `PermanentFaultException` | No (endpoint not found) |
| HTTP 429 with Retry-After | `RetryAfterException` | Yes |
| HTTP 5xx | `RuntimeException` | Yes |
| JSON-RPC error `-32600`, `-32601`, `-32602`, `-32700` | `PermanentFaultException` | No |
| JSON-RPC error `-32603` (internal error) | `RuntimeException` | Yes |
| JSON-RPC other error codes | `RuntimeException` | Yes |
| MCP result with `isError: true` | `RuntimeException` | Yes |
| Connection timeout | `RuntimeException` | Yes |
| Malformed response | `RuntimeException` | Yes |
| Initialization failure (version mismatch, 4xx) | `PermanentFaultException` | No |
| Initialization failure (connection error, 5xx) | `RuntimeException` | Yes |

---

## 8. Provisioner

### 8.1 getCapabilities()

Returns the full capability set from `McpServerResolver.capabilities()` — all `mcp:<server>:<tool>` tags derived from config.

### 8.2 provision()

Checks that at least one requested capability matches the resolved set. Validates that the resolved server has a non-blank URL (config completeness check — fail early on misconfiguration). Returns `ProvisionResult.empty()`. No external API call — no initialization at provisioning time.

### 8.3 terminate()

Returns `Uni.createFrom().voidItem()`. Nothing to tear down — session cleanup is handled by `McpSessionManager` on Quarkus shutdown, not per-worker termination.

### 8.4 Activation

`@ApplicationScoped` — displaces `NoOpReactiveWorkerProvisioner` by CDI priority when on classpath.

---

## 9. Testing Strategy

All tests are pure unit tests — Mockito + AssertJ, no `@QuarkusTest`, no network.

### 9.1 Server resolver

- Config with multiple servers → correct capability set built.
- `resolve()` with valid tag → correct `ResolvedMcpServer` returned.
- Multiple tags to same server → same `ResolvedMcpServer` instance returned (N:1).
- `resolve()` with unknown server → throws `WorkerProvisioningException`.
- `resolve()` with known server but unlisted tool → throws `WorkerProvisioningException`.
- Headers parsed correctly, including servers with no headers.
- Timeout falls back to global default when not specified per server.
- Missing or blank URL → `WorkerProvisioningException` at startup.
- Tools parsing: comma-separated, whitespace trimmed, empty entries ignored, duplicates rejected.

### 9.2 Session manager

- First dispatch triggers initialization handshake (initialize → initialized → session cached).
- Second dispatch to same server reuses cached session — no re-initialization.
- Session expiry (404) → cached session invalidated, next dispatch re-initializes.
- Concurrent initialization (two simultaneous dispatches to same server) → single initialization, both dispatches receive the same session.
- Initialization failure (transient — connection error) → cached Uni removed, next dispatch retries initialization.
- Initialization failure (permanent — version mismatch) → `PermanentFaultException` propagated.
- Protocol version negotiation: matching version → stored in session; mismatched version → PermanentFaultException.
- `Mcp-Session-Id` stored when server assigns one; omitted when server doesn't.
- Shutdown cleanup: DELETE sent for active sessions with session IDs.

### 9.3 Execution manager

- Successful tool call (JSON response) → `WorkflowExecutionCompleted` published with content in output.
- Successful tool call (SSE response) → JSON-RPC response extracted from SSE events, completion published.
- `structuredContent` present → used as completion output, `content` array ignored.
- `structuredContent` absent → `content` array used.
- InputData passed through as tool arguments.
- JSON-RPC request body correctly formed (method, id, params.name, params.arguments).
- Correct URL, protocol headers (`Accept`, `MCP-Protocol-Version`, `Mcp-Session-Id`), and auth headers sent.
- MCP `isError: true` → `RuntimeException` (retryable).
- JSON-RPC error `-32602` (invalid params) → `PermanentFaultException`.
- JSON-RPC error `-32603` (internal error) → `RuntimeException` (retryable).
- HTTP 404 with active session → session invalidated, `RuntimeException` (retryable).
- HTTP 429 with `Retry-After` → `RetryAfterException`.
- HTTP 4xx (except 429, 404-with-session) → `PermanentFaultException`.
- HTTP 5xx → `RuntimeException` (retryable).
- Malformed response → `RuntimeException` (retryable).
- Connection timeout → `notifications/cancelled` sent, `RuntimeException` (retryable).
- `schedulePersistedEvent` returns void item.
- `getActiveWorkCount` returns 0.

### 9.4 Fault event handler

- Permanent fault → skips retry, publishes exhausted.
- Retryable fault within max attempts → re-dispatches after delay.
- Retries exhausted → publishes exhausted.
- `RetryAfterException` → uses `retryAfterMs` as delay.

### 9.5 Provisioner

- Matching capability → `ProvisionResult.empty()`.
- Unmatched capability → throws.
- Resolved server with blank URL → throws (config completeness).

---

## 10. Co-deployment Constraints

Same constraint as other worker modules: CDI ambiguity on `WorkerExecutionManager` when co-deployed with workers-camel, workers-http, workers-github-actions, or scheduler-quartz. Blocked by engine#461 (composite `WorkerExecutionManager`).

`workerType = "mcp"` discriminator in `PendingCompletion` prevents event cross-talk if co-deployed, though this module does not register pending completions in v1.

---

## 11. Known Limitations

| Limitation | Description | Impact |
|------------|-------------|--------|
| No dynamic discovery | Worker does not call `tools/list` — tools must be declared in config | Operators must know which tools to expose. Filed as #7. |
| SSE buffered, not streamed | SSE responses are buffered entirely. Worker relies on server closing the stream after the response. | Long-running SSE streams with progress notifications timeout. No timeout clock reset on progress. |
| Server-to-client requests unsupported | Worker declares empty capabilities. If a server sends requests (sampling, elicitation) during a tool call, the worker ignores them. | Server's request times out on its end. Tool call can still complete. Well-behaved servers won't send these because capabilities are empty. |
| `structuredContent` used but not validated | Worker prefers `structuredContent` when present but does not validate against `outputSchema` | Invalid structured content propagates to completion output. |
| Single protocol version | Only `2025-06-18` supported. No backwards compatibility with `2024-11-05` HTTP+SSE transport. | Servers must support `2025-06-18`. |
| No progress notification handling | Progress notifications in SSE streams are ignored. Timeout clock is not reset. | Long-running tools must complete within the configured timeout. |

---

## 12. Future Evolution

- **Dynamic discovery (#7)** — call `tools/list` on configured servers at startup, auto-register all tools as capabilities. Reduces config burden, requires naming collision strategy.
- **MCP proxy/aggregator** — CaseHub exposes itself as an MCP server, aggregating tools from backend MCP servers. Inbound complement to this outbound worker. Logged in IDEAS.md.
- **SSE streaming** — replace buffered SSE parsing with Vert.x streaming API. Handle progress notifications (reset timeout clock). Handle server-to-client requests (requires declaring client capabilities).
- **Async completion** — for long-running tools, register pending completions and use MCP progress notifications over a persistent connection. Would add `McpCompletionExpiryObserver` and `McpFaultCallbackObserver`.
- **Tier 2 auth** — `EndpointRegistry` (platform#73) could provide per-capability credential resolution.
- **Multi-version support** — negotiate with older MCP servers via the backwards compatibility dance (POST initialize → on 4xx, fall back to GET for SSE endpoint event).
