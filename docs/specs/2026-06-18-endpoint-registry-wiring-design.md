# EndpointRegistry Wiring — Design Spec

**Issue:** casehubio/workers#12
**Date:** 2026-06-18
**Status:** Approved (revised after review — 7 findings addressed)
**Cross-repo:** engine#530 — add tenancyId to ProvisionContext; engine#531 — remove getCapabilities() hard gate in tryProvision()

---

## Problem

platform#73 shipped `EndpointRegistry` SPI in `casehub-platform-api`. Both `workers-http` (`HttpEndpointResolver`) and `workers-mcp` (`McpServerResolver`) have independent endpoint resolution that should consume the shared registry as a third resolution tier.

Four fundamental mismatches exist between the current resolver model and EndpointRegistry:

1. **Keying** — resolvers key by capability tag string; registry keys by `Path` + `tenancyId`
2. **Tenancy** — `WorkerCapabilityResolver.resolve(capabilityTag)` is tenant-blind; `EndpointRegistry.resolve(path, tenancyId)` is tenant-scoped
3. **Timing** — resolvers build a static HashMap at startup; registry endpoints can be registered at runtime
4. **Provisioner visibility** — `firstMatch(capabilities)` has no tenancy parameter; registry-only endpoints are invisible to the provisioner probe

## Approach

Extend `WorkerCapabilityResolver` to be tenancy-aware. Add `EndpointRegistry` as Tier 3 inside the resolvers. File engine#530 for `ProvisionContext.tenancyId` and engine#531 for the `getCapabilities()` hard gate to complete the provisioner path.

---

## Design

### 1. WorkerCapabilityResolver interface change

```java
// workers-common
public interface WorkerCapabilityResolver<T> {
    T resolve(String capabilityTag, String tenancyId);
    Optional<String> firstMatch(Set<String> capabilities, String tenancyId);
    Set<String> capabilities();  // static only — SPI + config
}
```

- `resolve()` and `firstMatch()` gain `tenancyId` — required, never null.
- `capabilities()` stays static — returns SPI + config endpoints only. Used by `WorkerRuntime.capabilities()` for lifecycle/status.
- Old single-arg signatures removed, not deprecated. All callers are in this repo.
- SPI-only and config-only tiers ignore tenancyId — the parameter passes through to Tier 3.
- **All four implementations must update** — HTTP, MCP gain registry integration; Camel, Script pass tenancyId through without using it.

### 2. Resolution order and tenant fallback

Each resolver's `resolve(capabilityTag, tenancyId)` follows this precedence:

```
Tier 1: SPI beans         (highest — code-defined, static)
Tier 2: Config properties (deploy-time, static)
Tier 3: EndpointRegistry  (runtime, tenant-scoped)
```

For Tier 3, the resolver makes a **single call**: `registry.resolve(path, tenancyId)`. The registry implementation handles tenant → platform-global fallback internally. `InMemoryEndpointRegistry.resolve()` already implements this: tries tenant-specific first, falls back to `PLATFORM_TENANT_ID`. This is the expected contract for all registry implementations — the worker does not make two calls.

`firstMatch(capabilities, tenancyId)` follows the same order — checks the static capability set first, then probes the registry for each unmatched tag.

When `EndpointRegistry` is `NoOpEndpointRegistry @DefaultBean` (no implementation on classpath), `resolve()` returns `Optional.empty()`. Tier 3 is a no-op. Existing behavior unchanged for deployments without EndpointRegistry.

### 3. Path convention

Deterministic mapping from capability tag to registry `Path`:

| Worker type | Capability tag | Registry path | Lookup |
|---|---|---|---|
| HTTP | `send-email` | `Path.of("http", "send-email")` | `registry.resolve(Path.of("http", tag), tenancyId)` |
| MCP | `mcp:slack:send-message` | `Path.of("mcp", "slack")` | `registry.resolve(Path.of("mcp", serverName), tenancyId)` |

HTTP maps 1:1 — one capability tag, one endpoint descriptor.

MCP maps N:1 — the registry entry is per-server, not per-tool. The tool set comes from config or `tools/list` discovery. The registry provides the server definition (URL, headers, timeout).

Path prefixes (`http/`, `mcp/`) scope by protocol, preventing collisions. Module-local string literals, not `EndpointPropertyKeys` constants (per `spi-property-keys-cross-module-only` protocol).

### 4. EndpointDescriptor → resolved type mapping

**HTTP: `EndpointDescriptor` → `ResolvedEndpoint`**

| Field | Source | Key |
|---|---|---|
| url | `properties` | `EndpointPropertyKeys.URL` (cross-module) |
| method | `properties` | `"method"` (module-local, default `"POST"`) |
| mode | `properties` | `"mode"` (module-local, default `"SYNC"`) |
| timeout | `properties` | `"timeout-seconds"` (module-local, default from config) |
| headers | `properties` | `"headers.*"` prefix (module-local) |

Validation: missing or blank URL → `WorkerProvisioningException`.

**Protocol check:** HTTP resolver verifies `descriptor.protocol() == EndpointProtocol.HTTP`. Wrong protocol → ignored (returns empty), not faulted.

**MCP: `EndpointDescriptor` → `ResolvedMcpServer`**

| Field | Source | Key |
|---|---|---|
| name | `path` last segment | Derived from `Path.of("mcp", name)` |
| url | `properties` | `EndpointPropertyKeys.URL` (cross-module) |
| timeout | `properties` | `"timeout-seconds"` (module-local, default from config) |
| headers | `properties` | `"headers.*"` prefix (module-local) |
| tools | `properties` | `"tools"` (module-local, comma-separated or empty for discovery) |
| discovery | `properties` | `"discovery"` (module-local, default `"auto"`) |

Validation: missing or blank URL → `WorkerProvisioningException`.

**Protocol check:** MCP resolver verifies `descriptor.protocol() == EndpointProtocol.MCP`. Wrong protocol → ignored (returns empty), not faulted.

**MCP discovery timing for registry-resolved servers:** When `McpServerResolver.resolve()` finds a server via EndpointRegistry at dispatch time, the server was not initialized at startup — no `McpSession` exists and no `tools/list` discovery has run. The resolver builds a `ResolvedMcpServer` from the descriptor's properties (URL, headers, timeout, tools). If `discovery=auto` and `tools` is empty, the tool set is empty — the resolver returns the server, and `McpSessionManager.getOrInitialize()` lazily creates the session at dispatch time (existing infrastructure). Tool discovery runs as part of session initialization. If `discovery=manual` or `tools` is populated, the configured tools are used directly. This matches the existing lazy infrastructure — `getOrInitialize()` was designed for dispatch-time re-init after 404.

**MCP firstMatch() semantics for registry-resolved servers (design decision):** For MCP Tier 3, `firstMatch()` validates **server existence** via registry lookup, not individual tool existence. The capability tag `mcp:slack:send-message` maps to registry path `Path.of("mcp", "slack")`. If the registry has a descriptor for `slack`, `firstMatch()` reports the capability as available — even though tool `send-message` hasn't been validated against the server's tool set.

This is a deliberate weaker guarantee than Tier 1/2 (which validate against a known tool set). The consequence chain: provisioner reports "available" → engine dispatches → `McpSessionManager.getOrInitialize()` creates session → `tools/list` discovers tools → dispatch faults if tool doesn't exist on the server. The fault enters the retry pipeline.

This is the same lazy validation pattern as 404 session recovery — server reachability and tool availability are verified at dispatch time, not probe time. The provisioner is a cheap probe, not a contract guarantee.

### 5. Provisioner tenancyId handling

Each provisioner extracts tenancyId inline:

```java
// In each provisioner's provision() method:
String tenancyId = TenancyConstants.PLATFORM_TENANT_ID; // engine#530: context.tenancyId()
resolver.firstMatch(capabilities, tenancyId).orElseThrow(...);
```

When engine#530 ships and `ProvisionContext.tenancyId()` is available, update each call site to `context.tenancyId()`. Four call sites — mechanical grep-and-replace.

**Dispatch path** (`submit()` → `resolve(tag, tenancyId)`): fully tenant-aware from day one via `CaseInstance.tenancyId`.

**Provisioner probe** (`provision()` → `firstMatch(caps, tenancyId)`): checks static capabilities first, then probes registry with `PLATFORM_TENANT_ID` until engine#530 ships.

### 6. Provisioner getCapabilities() gap (engine-side)

`getCapabilities()` remains static — returns `resolver.capabilities()`. Registry endpoints are contextual, not lifecycle advertisements.

**Known limitation:** The engine's `CaseContextChangedEventHandler.tryProvision()` hard-gates on `getCapabilities()` — if the capability isn't in the static set, `provision()` is never called. This means registry-only endpoints are unreachable via the provisioner **fallback** path.

This does NOT affect the **primary dispatch path**. `tryProvision()` is a fallback called only when: (1) no workers defined in the case definition, (2) no eligible workers found, or (3) routing returns `Unresolvable`. The primary path goes through `WorkerScheduleEvent` → `WorkerExecutionManager.submit()` → `resolver.resolve(tag, tenancyId)` — which is where our EndpointRegistry wiring lives and works fully.

**Fix:** engine#531 — remove the `getCapabilities()` hard gate. Let `provision()` decide — it has full context (capabilities set, tenancyId, registry access). The engine already handles `ProvisioningException` at the end of `tryProvision()`.

### 7. Dependency changes

| Module | Change |
|---|---|
| workers-common | No POM change (already depends on `casehub-platform-api`) |
| workers-http | No POM change (already depends on `casehub-platform-api`) |
| workers-mcp | **Add `casehub-platform-api` compile dependency** |
| workers-camel | No POM change |
| workers-script | No POM change |

EndpointRegistry is injected directly into HTTP and MCP resolvers. `NoOpEndpointRegistry @DefaultBean` activates when no implementation is on classpath — no configuration required.

Test-friendly `initialize()` methods on HTTP and MCP resolvers gain an `EndpointRegistry` parameter. Tests pass a stub or `NoOpEndpointRegistry`.

### 8. File change manifest

| File | Module | Change |
|---|---|---|
| `WorkerCapabilityResolver.java` | workers-common | Add `tenancyId` to `resolve()`, `firstMatch()` |
| `HttpEndpointResolver.java` | workers-http | Inject `EndpointRegistry`, Tier 3 resolution, update `initialize()` |
| `HttpWorkerExecutionManager.java` | workers-http | Pass `instance.tenancyId` to `resolve()` |
| `HttpReactiveWorkerProvisioner.java` | workers-http | Inline tenancyId, pass to `firstMatch()` and `resolve()` |
| `HttpEndpointResolverTest.java` | workers-http | Tier 3 tests: hit, miss, tenant fallback, protocol mismatch, NoOp |
| `HttpReactiveWorkerProvisionerTest.java` | workers-http | tenancyId parameter |
| `HttpWorkerExecutionManagerTest.java` | workers-http | tenancyId in `resolve()` call |
| `McpServerResolver.java` | workers-mcp | Inject `EndpointRegistry`, registry-backed resolution, update `initialize()` |
| `McpWorkerExecutionManager.java` | workers-mcp | Pass `instance.tenancyId` to `resolve()` |
| `McpReactiveWorkerProvisioner.java` | workers-mcp | Inline tenancyId, pass to `firstMatch()` |
| `McpServerResolverTest.java` | workers-mcp | Tier 3 tests: hit, miss, tenant fallback, tools/discovery |
| `McpWorkerExecutionManagerTest.java` | workers-mcp | tenancyId in `resolve()` call |
| `McpWorkerExecutionManagerTest.java` | workers-mcp | tenancyId in `resolve()` mock |
| `McpReactiveWorkerProvisionerTest.java` | workers-mcp | tenancyId parameter |
| `pom.xml` | workers-mcp | Add `casehub-platform-api` dependency |
| `CamelCapabilityResolver.java` | workers-camel | Update `resolve()`, `firstMatch()` signatures (pass tenancyId through) |
| `CamelWorkerExecutionManager.java` | workers-camel | Pass `instance.tenancyId` to `resolve()` |
| `CamelReactiveWorkerProvisioner.java` | workers-camel | Inline tenancyId, pass to `firstMatch()` and `resolve()` |
| `CamelCapabilityResolverTest.java` | workers-camel | tenancyId parameter |
| `CamelWorkerExecutionManagerTest.java` | workers-camel | tenancyId in `resolve()` call |
| `CamelReactiveWorkerProvisionerTest.java` | workers-camel | tenancyId parameter |
| `ScriptDefinitionResolver.java` | workers-script | Update `resolve()`, `firstMatch()` signatures (pass tenancyId through) |
| `ScriptWorkerExecutionManager.java` | workers-script | Pass `instance.tenancyId` to `resolve()` |
| `ScriptReactiveWorkerProvisioner.java` | workers-script | Inline tenancyId, pass to `firstMatch()` and `resolve()` |
| `ScriptDefinitionResolverTest.java` | workers-script | tenancyId parameter |
| `ScriptWorkerExecutionManagerTest.java` | workers-script | tenancyId in `resolve()` call |
| `ScriptReactiveWorkerProvisionerTest.java` | workers-script | tenancyId parameter |

**Not changed:** workers-github-actions (fixed API endpoint — GitHub API is not a configurable destination), WorkerRuntime implementations (delegate to static `capabilities()`).

---

## Out of scope

- `credentialRef` resolution — deferred until a secrets backend resolver is implemented
- GitHub Actions worker module — GitHub API is a fixed endpoint, not a configurable destination
- `EndpointRegistry` implementations — `NoOpEndpointRegistry` and `InMemoryEndpointRegistry` already exist in casehub-platform
- YAML endpoint populator — `casehub-platform-endpoints-config` already handles YAML loading into the registry

## Cross-repo dependencies

| Issue | Repo | What | Impact if unresolved |
|---|---|---|---|
| engine#530 | casehub-engine | Add `tenancyId` to `ProvisionContext` | Provisioner probe limited to platform-global endpoints |
| engine#531 | casehub-engine | Remove `getCapabilities()` hard gate in `tryProvision()` | Registry-only endpoints unreachable via provisioner fallback path (primary dispatch path unaffected) |
