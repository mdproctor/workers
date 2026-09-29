package io.casehub.workers.camel.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import io.casehub.engine.common.internal.model.CaseInstance;

import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.CompletionExpiredEvent;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CasehubProducerTest {

    private CasehubProducer producer;
    private AsyncWorkerCompletionRegistry registry;
    private WorkflowCompletionPublisher completionPublisher;
    private WorkerFaultPublisher faultPublisher;
    private PendingCompletion testPending;

    @BeforeEach
    void setUp() {
        registry = mock(AsyncWorkerCompletionRegistry.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        faultPublisher = mock(WorkerFaultPublisher.class);

        // Create test pending completion
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = Worker.builder().name("w1").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash", "t1", null);
        testPending = new PendingCompletion(
            "test-dispatch-id",
            "camel",
            ctx,
            UUID.randomUUID().toString(),
            Capability.of("cap", "", ""),
            1L,
            Instant.now(),
            Instant.now().plus(Duration.ofMinutes(60)),
            Map.of()
        );

        CasehubEndpoint endpoint = new CasehubEndpoint("casehub:complete", new CasehubComponent());
        producer = new CasehubProducer(endpoint, registry, completionPublisher, faultPublisher);
    }

    @Test
    void process_successfulCompletion() throws Exception {
        when(registry.complete(testPending.dispatchId())).thenReturn(Optional.of(testPending));
        Exchange exchange = createExchange(testPending.dispatchId());
        exchange.getIn().setBody(Map.of("result", "ok"));

        producer.process(exchange);

        verify(completionPublisher).complete(eq(testPending.correlationContext()), eq(Map.of("result", "ok")));
        verifyNoInteractions(faultPublisher);
    }

    @Test
    void process_faultedViaException() throws Exception {
        when(registry.complete(testPending.dispatchId())).thenReturn(Optional.of(testPending));
        Exchange exchange = createExchange(testPending.dispatchId());
        exchange.setException(new RuntimeException("boom"));

        producer.process(exchange);

        verify(faultPublisher).fault(eq(testPending), any(RuntimeException.class));
        verifyNoInteractions(completionPublisher);
    }

    @Test
    void process_faultedViaHeader() throws Exception {
        when(registry.complete(testPending.dispatchId())).thenReturn(Optional.of(testPending));
        Exchange exchange = createExchange(testPending.dispatchId());
        exchange.getIn().setHeader(CasehubWorkerHeaders.WORK_STATUS, "FAULTED");

        producer.process(exchange);

        verify(faultPublisher).fault(eq(testPending), isNull());
        verifyNoInteractions(completionPublisher);
    }

    @Test
    void process_missingWorkerId_throws() {
        Exchange exchange = new DefaultCamelContext().getEndpoint("direct:test").createExchange();

        assertThatThrownBy(() -> producer.process(exchange))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("casehub-worker-id");
    }

    @Test
    void process_unknownDispatchId_noOps() throws Exception {
        when(registry.complete("unknown-id")).thenReturn(Optional.empty());
        Exchange exchange = createExchange("unknown-id");
        exchange.getIn().setBody(Map.of());

        producer.process(exchange);

        verify(registry).complete("unknown-id");
        verifyNoInteractions(completionPublisher);
        verifyNoInteractions(faultPublisher);
    }

    private Exchange createExchange(String dispatchId) {
        Exchange exchange = new DefaultCamelContext().getEndpoint("direct:test").createExchange();
        exchange.getIn().setHeader(CasehubWorkerHeaders.WORKER_ID, dispatchId);
        return exchange;
    }
}
