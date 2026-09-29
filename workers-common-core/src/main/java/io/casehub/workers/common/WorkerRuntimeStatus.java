package io.casehub.workers.common;

/**
 * Lifecycle status of a {@link WorkerRuntime}.
 *
 * <p>Aligned with the status vocabulary used across CaseHub and Serverless
 * Workflow 1.0 where applicable. Workflow/task instances use
 * {@code WorkflowStatus} (PENDING, RUNNING, WAITING, COMPLETED, FAULTED,
 * CANCELLED, SUSPENDED); worker runtimes use a subset appropriate to an
 * executor lifecycle rather than a task instance lifecycle.
 *
 * <p>Current states cover the initialization and shutdown lifecycle.
 * Future states may include:
 * <ul>
 *   <li>{@code SUSPENDED} — temporarily not accepting dispatches (e.g.,
 *       backpressure, maintenance window). Transitions: RUNNING → SUSPENDED
 *       → RUNNING.</li>
 *   <li>{@code DRAINING} — no new dispatches accepted, in-flight work
 *       completing before shutdown. Transition: RUNNING → DRAINING →
 *       STOPPED.</li>
 * </ul>
 *
 * <p>When adding states, preserve the convention: states that accept new
 * dispatches are "active" (currently only {@code RUNNING}); states that
 * reject new dispatches are "inactive" ({@code PENDING}, {@code FAULTED},
 * {@code STOPPED}).
 */
public enum WorkerRuntimeStatus {
    /** Configured but not yet initialized. Initial state. */
    PENDING,
    /** Initialized and accepting dispatches. */
    RUNNING,
    /** Initialization failed or a runtime error made the worker unavailable. */
    FAULTED,
    /** Shutdown completed. Terminal state. */
    STOPPED
}
