package io.casehub.workers.common;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerLifecycleOrchestratorTest {

    @Test
    void initializeAll_parallelExecution() {
        var rt1 = new StubRuntime("rt1", 200);
        var rt2 = new StubRuntime("rt2", 200);
        var rt3 = new StubRuntime("rt3", 200);

        var orchestrator = new WorkerLifecycleOrchestrator(
            List.of(rt1, rt2, rt3), Duration.ofSeconds(5));

        long start = System.nanoTime();
        orchestrator.initializeAll();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(rt1.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(rt2.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(rt3.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(elapsedMs).isLessThan(500);
    }

    @Test
    void initializeAll_timeoutDoesNotBlockOthers() {
        var fast = new StubRuntime("fast", 0);
        var slow = new StubRuntime("slow", 5000);

        var orchestrator = new WorkerLifecycleOrchestrator(
            List.of(slow, fast), Duration.ofMillis(500));

        long start = System.nanoTime();
        orchestrator.initializeAll();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(fast.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(elapsedMs).isLessThan(1500);
    }

    @Test
    void initializeAll_faultIsolation() {
        var good = new StubRuntime("good", 0);
        var bad = new FailingRuntime("bad");

        var orchestrator = new WorkerLifecycleOrchestrator(
            List.of(bad, good), Duration.ofSeconds(5));

        orchestrator.initializeAll();

        assertThat(good.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(bad.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initializeAll_emptyList() {
        var orchestrator = new WorkerLifecycleOrchestrator(
            List.of(), Duration.ofSeconds(5));
        orchestrator.initializeAll();
    }

    @Test
    void shutdownAll_skipsPendingRuntimes() {
        var pending = new StubRuntime("pending", 0);

        var orchestrator = new WorkerLifecycleOrchestrator(
            List.of(pending), Duration.ofSeconds(5));
        orchestrator.shutdownAll();

        assertThat(pending.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
    }

    @Test
    void shutdownAll_stopsRunningRuntimes() {
        var rt = new StubRuntime("rt", 0);
        var orchestrator = new WorkerLifecycleOrchestrator(
            List.of(rt), Duration.ofSeconds(5));

        orchestrator.initializeAll();
        assertThat(rt.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);

        orchestrator.shutdownAll();
        assertThat(rt.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }

    static class StubRuntime implements WorkerRuntime {
        private final String type;
        private final long initDelayMs;
        private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

        StubRuntime(String type, long initDelayMs) {
            this.type = type;
            this.initDelayMs = initDelayMs;
        }

        @Override public String workerType() { return type; }
        @Override public WorkerRuntimeStatus status() { return status; }
        @Override public Set<String> capabilities() { return Set.of(type); }

        @Override
        public void initialize() {
            try { Thread.sleep(initDelayMs); } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); return;
            }
            status = WorkerRuntimeStatus.RUNNING;
        }

        @Override
        public void shutdown() { status = WorkerRuntimeStatus.STOPPED; }
    }

    static class FailingRuntime implements WorkerRuntime {
        private final String type;
        private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

        FailingRuntime(String type) { this.type = type; }

        @Override public String workerType() { return type; }
        @Override public WorkerRuntimeStatus status() { return status; }
        @Override public Set<String> capabilities() { return Set.of(); }

        @Override
        public void initialize() {
            status = WorkerRuntimeStatus.FAULTED;
            throw new RuntimeException("init failed");
        }

        @Override
        public void shutdown() { status = WorkerRuntimeStatus.STOPPED; }
    }
}
