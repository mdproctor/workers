# Migrate Worker Imports to casehub-worker-api

**Issue:** casehubio/workers#14
**Status:** Approved
**Date:** 2026-06-25

## Context

The Worker Foundation Extraction (casehubio/casehub-desiredstate#40) created
`casehub-worker-api` — a standalone artifact containing the Worker domain
primitives as Java records. Engine has already migrated (engine#543 CLOSED)
and the BOM includes worker artifacts (parent#288 CLOSED).

**This migration is mandatory, not voluntary.** `WorkerExecutionManager.submit()`
in engine-common already takes `io.casehub.worker.api.Worker` and
`io.casehub.worker.api.Capability`. Workers implementations still import the old
`io.casehub.api.model` types — different classes that no longer satisfy the SPI
contract. The build breaks on the next engine-common SNAPSHOT pull.

## Complete Migration Map

The issue's mapping table covers 6 types. The codebase uses 11 distinct types
from `io.casehub.api.model.*`. Five were missing from the issue.

### To casehub-worker-api

| Old | New | API change |
|-----|-----|------------|
| `io.casehub.api.model.Worker` | `io.casehub.worker.api.Worker` | Record. `getName()` → `name()`, `getExecutionPolicy()` → `executionPolicy()`. `getCapabilities()` accessor exists but has zero call sites in workers. |
| `io.casehub.api.model.Capability` | `io.casehub.worker.api.Capability` | Record. 3-arg constructor gone → use `Capability.of(name, in, out)`. `getName()` → `name()` |

### To casehub-platform-api governance

| Old | New | API change |
|-----|-----|------------|
| `io.casehub.api.model.ExecutionPolicy` | `io.casehub.platform.api.governance.ExecutionPolicy` | Import-only swap |
| `io.casehub.api.model.RetryPolicy` | `io.casehub.platform.api.governance.RetryPolicy` | Import-only swap |
| `io.casehub.api.model.BackoffStrategy` | `io.casehub.platform.api.governance.BackoffStrategy` | Import-only swap |

### Stay in engine-api (no change)

| Type | Reason |
|------|--------|
| `io.casehub.api.model.ProvisionContext` | Engine SPI parameter |
| `io.casehub.api.model.WorkResult` | Orchestration-level result (distinct from WorkerResult) |
| `io.casehub.api.model.event.CaseHubEventType` | Engine event model |
| `io.casehub.api.model.event.EventStreamType` | Engine event model |
| `io.casehub.api.spi.ReactiveWorkerProvisioner` | Engine SPI interface |

## Breaking Changes

### 1. Capability Constructor (28 sites)

Old `Capability` class had a 3-arg constructor `(name, inputSchema, outputSchema)`.
New `Capability` record has a 4-arg constructor `(name, inputSchema, outputSchema, description)`.

Every `new Capability(tag, "", "")` becomes `Capability.of(tag, "", "")`.

The factory sets `description = null`. The compact constructor enforces
`Objects.requireNonNull` on `inputSchema` and `outputSchema` — all existing
call sites use `""` (not null), so GE-20260624-0b931d does not trigger.

### 2. Worker.Builder.function() Signature (20 sites in 17 files)

Old: `function(Function<Map<String,Object>, Map<String,Object>>)` — returns output map.
New: two overloads —
- `function(WorkerFunction)` — the SPI interface
- `function(Function<Map<String,Object>, WorkerResult>)` — wraps in `WorkerFunction.Sync`

Two breaks in test code:
1. **Return type**: `ctx -> null` fails — return type is now `WorkerResult`, not `Map`
2. **Lambda ambiguity** (GE-20260624-3324b6): a lambda matches both overloads

Fix: wrap explicitly with `new WorkerFunction.Sync(ctx -> WorkerResult.of(Map.of()))`.

Each of the 17 files gains two new imports (`WorkerFunction`, `WorkerResult`)
that don't exist today — 34 net new import statements on top of the 70 swaps.

### 3. Record Accessor Names (31 sites)

| Old | New | Call sites |
|-----|-----|-----------|
| `worker.getName()` | `worker.name()` | 17 |
| `capability.getName()` | `capability.name()` | 13 |
| `worker.getExecutionPolicy()` | `worker.executionPolicy()` | 1 |

## POM Changes

`casehub-worker-api` is already transitively available via `casehub-engine-api`
(engine-api declares worker-api as a direct dependency). The additions below
make the direct dependency explicit per Maven best practice — the code would
compile without them, but implicit transitive dependencies break silently
when intermediaries reorganise.

### Parent POM — add to dependencyManagement

```xml
<dependency>
    <groupId>io.casehub</groupId>
    <artifactId>casehub-worker-api</artifactId>
    <version>${version.io.casehub}</version>
</dependency>
```

### Child modules — add direct dependency

All 7 modules (workers-common, workers-http, workers-camel,
workers-github-actions, workers-mcp, workers-script, workers-testing)
import worker-api types directly. Each gets:

```xml
<dependency>
    <groupId>io.casehub</groupId>
    <artifactId>casehub-worker-api</artifactId>
</dependency>
```

Governance types (ExecutionPolicy, RetryPolicy, BackoffStrategy) are only
directly imported in workers-common, which already declares
`casehub-platform-api`. No POM change needed for governance type swaps.

## Scope

| Dimension | Count |
|-----------|-------|
| Import swaps | 70 swaps + 34 new (WorkerFunction, WorkerResult) = 104 across ~35 files |
| Getter renames | 31 call sites (worker.name 17, capability.name 13, worker.executionPolicy 1) |
| Capability construction | 28 sites |
| Worker.builder().function() | 20 sites in 17 files |
| POM changes | 8 files (1 parent + 7 modules) |
| Production files | ~13 |
| Test files | ~22 |

## Risk

Low. Every change is compile-time verifiable. No behavioral differences.
The new types are records with identical semantics. The null-rejection
constraint on Capability schemas does not trigger because tests use `""`
not `null`.

## Garden References

- GE-20260624-0b931d: Capability record rejects null inputSchema/outputSchema
- GE-20260624-3324b6: Worker.Builder.function(lambda) ambiguity with two overloads

## Protocol Coherence

- PP-20260529-ce2de0 (engine-api-scope-rule): workers continues to depend on
  engine-api for ProvisionContext and event types. Worker primitives move to
  worker-api. Governance types move to platform-api. Clean separation.
- PP-20260512-coord (maven-coordinate-standard): casehub-worker-api follows
  `casehub-{repo}-{function}` naming. groupId is `io.casehub`.
- PP-20260512-arename (artifact-rename-propagation): no artifact rename — this
  is a new dependency addition, not a rename.
