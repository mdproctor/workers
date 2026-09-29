package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import io.casehub.workers.common.WorkerFaultEvent;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class WorkerFaultPublisherTest {

    @Test
    void fault_fromContext_publishesToGivenAddress() {
        @SuppressWarnings("unchecked") Consumer<WorkerFaultEvent> consumer = mock(Consumer.class);
        WorkerFaultPublisher publisher = new WorkerFaultPublisher(consumer);

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = Worker.builder().name("w1").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1", null);
        Capability capability = Capability.of("run-script", "", "");

        publisher.fault(ctx, capability, 99L, new RuntimeException("boom"));

        ArgumentCaptor<WorkerFaultEvent> captor = ArgumentCaptor.forClass(WorkerFaultEvent.class);
        verify(consumer).accept(captor.capture());

        WorkerFaultEvent event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.capability()).isSameAs(capability);
        assertThat(event.inputDataHash()).isEqualTo("hash-1");
        assertThat(event.eventLogId()).isEqualTo("99");
    }

    @Test
    void fault_fromPendingCompletion_publishesToFaultAddress() {
        @SuppressWarnings("unchecked") Consumer<WorkerFaultEvent> consumer = mock(Consumer.class);
        WorkerFaultPublisher publisher = new WorkerFaultPublisher(consumer);

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = Worker.builder().name("w1").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1", null);
        Capability capability = Capability.of("send-webhook", "", "");
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "http",
            ctx, "token", capability, 42L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
        Throwable cause = new RuntimeException("timeout");

        publisher.fault(pending, cause);

        ArgumentCaptor<WorkerFaultEvent> captor = ArgumentCaptor.forClass(WorkerFaultEvent.class);
        verify(consumer).accept(captor.capture());

        WorkerFaultEvent event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.capability()).isSameAs(capability);
        assertThat(event.inputDataHash()).isEqualTo("hash-1");
        assertThat(event.eventLogId()).isEqualTo("42");
        assertThat(event.cause()).isSameAs(cause);
    }

    @Test
    void fault_address_passesBindingNameFromContext() {
        @SuppressWarnings("unchecked") Consumer<WorkerFaultEvent> consumer = mock(Consumer.class);
        WorkerFaultPublisher publisher = new WorkerFaultPublisher(consumer);

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = Worker.builder().name("w1").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, "hash-1", "t1", "binding-x");
        Capability capability = Capability.of("test-cap", "", "");
        Throwable cause = new RuntimeException("test");

        publisher.fault(ctx, capability, 42L, cause);

        ArgumentCaptor<WorkerFaultEvent> captor = ArgumentCaptor.forClass(WorkerFaultEvent.class);
        verify(consumer).accept(captor.capture());
        assertThat(captor.getValue().bindingName()).isEqualTo("binding-x");
    }

    @Test
    void fault_pending_passesBindingNameFromContext() {
        @SuppressWarnings("unchecked") Consumer<WorkerFaultEvent> consumer = mock(Consumer.class);
        WorkerFaultPublisher publisher = new WorkerFaultPublisher(consumer);

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = Worker.builder().name("w1").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, "hash-1", "t1", "binding-y");
        Capability capability = Capability.of("test-cap", "", "");
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "http",
            ctx, "token", capability, 42L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
        Throwable cause = new RuntimeException("test");

        publisher.fault(pending, cause);

        ArgumentCaptor<WorkerFaultEvent> captor = ArgumentCaptor.forClass(WorkerFaultEvent.class);
        verify(consumer).accept(captor.capture());
        assertThat(captor.getValue().bindingName()).isEqualTo("binding-y");
    }
}
