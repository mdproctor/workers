package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;

import org.junit.jupiter.api.Test;

class AsyncWorkerCompletionRegistryTest {

    private AsyncWorkerCompletionRegistry registry;
    private List<CompletionExpiredEvent> firedEvents;

    @BeforeEach
    void setUp() {
        firedEvents = new CopyOnWriteArrayList<>();
        Consumer<CompletionExpiredEvent> capturingConsumer = firedEvents::add;
        registry = new AsyncWorkerCompletionRegistry(capturingConsumer);
    }

    @Test
    void register_generatesUniqueDispatchIdAndCallbackToken() {
        WorkerCorrelationContext ctx = testContext();
        PendingCompletion p1 = registry.register("camel", ctx, testCapability(), 1L, Duration.ofMinutes(60), Map.of());
        PendingCompletion p2 = registry.register("camel", ctx, testCapability(), 2L, Duration.ofMinutes(60), Map.of());
        assertThat(p1.dispatchId()).isNotEqualTo(p2.dispatchId());
        assertThat(p1.callbackToken()).isNotEqualTo(p2.callbackToken());
        assertThat(p1.workerType()).isEqualTo("camel");
    }

    @Test
    void complete_returnsAndRemoves() {
        PendingCompletion pending = registry.register("camel", testContext(), testCapability(), 1L, Duration.ofMinutes(60), Map.of());
        Optional<PendingCompletion> completed = registry.complete(pending.dispatchId());
        assertThat(completed).isPresent().contains(pending);
        Optional<PendingCompletion> second = registry.complete(pending.dispatchId());
        assertThat(second).isEmpty();
    }

    @Test
    void complete_unknownDispatchId_returnsEmpty() {
        assertThat(registry.complete("nonexistent")).isEmpty();
    }

    @Test
    void countByWorkerName_countsActiveDispatches() {
        WorkerCorrelationContext ctx = testContext();
        registry.register("camel", ctx, testCapability(), 1L, Duration.ofMinutes(60), Map.of());
        registry.register("camel", ctx, testCapability(), 2L, Duration.ofMinutes(60), Map.of());
        assertThat(registry.countByWorkerName(ctx.worker().name())).isEqualTo(2);
    }

    @Test
    void expireStale_firesEventForExpiredEntries() {
        WorkerCorrelationContext ctx = testContext();
        registry.register("camel", ctx, testCapability(), 1L, Duration.ofSeconds(-1), Map.of());
        PendingCompletion live = registry.register("camel", ctx, testCapability(), 2L, Duration.ofMinutes(60), Map.of());
        registry.expireStale();
        assertThat(firedEvents).hasSize(1);
        assertThat(firedEvents.get(0).pending().eventLogId()).isEqualTo(1L);
        assertThat(registry.complete(live.dispatchId())).isPresent();
    }

    private WorkerCorrelationContext testContext() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = Worker.builder().name("test-worker").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        return new WorkerCorrelationContext(instance, worker, "hash", "t1", null);
    }

    private Capability testCapability() {
        return Capability.of("send-email", "", "");
    }

}
