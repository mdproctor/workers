package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import io.casehub.engine.common.internal.model.CaseInstance;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkerCallbackResourceTest {

    private WorkerCallbackResource resource;
    private AsyncWorkerCompletionRegistry registry;
    private WorkflowCompletionPublisher completionPublisher;
    private List<FaultCallbackEvent> faultEvents;

    @BeforeEach
    void setUp() {
        registry = new AsyncWorkerCompletionRegistry(e -> {});

        completionPublisher = mock(WorkflowCompletionPublisher.class);
        faultEvents = new CopyOnWriteArrayList<>();

        resource = new WorkerCallbackResource();
        resource.registry = registry;
        resource.completionPublisher = completionPublisher;
        resource.faultCallbackEvents = new CapturingEvent<>(faultEvents);
    }

    @SuppressWarnings("unchecked")
    static class CapturingEvent<T> implements jakarta.enterprise.event.Event<T> {
        private final List<T> captured;
        CapturingEvent(List<T> captured) { this.captured = captured; }
        @Override public void fire(T event) { captured.add(event); }
        @Override public <U extends T> java.util.concurrent.CompletionStage<U> fireAsync(U event) {
            captured.add(event);
            return java.util.concurrent.CompletableFuture.completedFuture(event);
        }
        @Override public <U extends T> java.util.concurrent.CompletionStage<U> fireAsync(U event, jakarta.enterprise.event.NotificationOptions options) { return fireAsync(event); }
        @Override public jakarta.enterprise.event.Event<T> select(java.lang.annotation.Annotation... qualifiers) { return this; }
        @Override public <U extends T> jakarta.enterprise.event.Event<U> select(Class<U> subtype, java.lang.annotation.Annotation... qualifiers) { return (jakarta.enterprise.event.Event<U>) this; }
        @Override public <U extends T> jakarta.enterprise.event.Event<U> select(jakarta.enterprise.util.TypeLiteral<U> subtype, java.lang.annotation.Annotation... qualifiers) { return (jakarta.enterprise.event.Event<U>) this; }
    }

    @Test
    void successfulCompletion_callsPublisher() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(Map.of("key", "value"), false, null);
        Response response = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        assertThat(response.getStatus()).isEqualTo(200);
        verify(completionPublisher).complete(eq(pending.correlationContext()), eq(Map.of("key", "value")));
    }

    @Test
    void faultedCompletion_firesFaultCallbackEvent() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, true, "route failed");
        Response response = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(faultEvents).hasSize(1);
        assertThat(faultEvents.get(0).pending()).isEqualTo(pending);
        assertThat(faultEvents.get(0).cause().getMessage()).isEqualTo("route failed");
        verifyNoInteractions(completionPublisher);
    }

    @Test
    void unknownDispatchId_returns404() {
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, false, null);
        Response response = resource.complete("unknown", "token", payload);
        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void wrongCallbackToken_returns401() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, false, null);
        Response response = resource.complete(pending.dispatchId(), "wrong-token", payload);
        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void duplicateCompletion_returns404() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(Map.of(), false, null);
        resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        Response second = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        assertThat(second.getStatus()).isEqualTo(404);
    }

    private PendingCompletion registerTestPending() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = Worker.builder().name("w1").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash", "t1", null);
        return registry.register("camel", ctx, Capability.of("cap", "", ""), 1L, Duration.ofMinutes(60), Map.of());
    }
}
