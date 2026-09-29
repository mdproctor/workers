package io.casehub.workers.common;

import java.util.Set;

/**
 * Lifecycle contract for a worker runtime — the infrastructure that executes
 * dispatched work for a specific worker type.
 *
 * <p>Implementations are blocking. The lifecycle orchestrator runs all
 * runtimes in parallel on virtual threads with per-runtime timeouts —
 * implementations do not need to manage their own threading.
 */
public interface WorkerRuntime {

    String workerType();

    WorkerRuntimeStatus status();

    void initialize();

    void shutdown();

    Set<String> capabilities();
}
