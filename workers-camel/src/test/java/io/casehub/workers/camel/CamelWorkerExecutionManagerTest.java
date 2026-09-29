package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CamelWorkerExecutionManagerTest {

    private CamelWorkerExecutionManager manager;
    private CamelCapabilityResolver resolver;
    private WorkerFaultPublisher faultPublisher;
    private AsyncWorkerCompletionRegistry registry;

    @BeforeEach
    void setUp() {
        resolver = mock(CamelCapabilityResolver.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        registry = mock(AsyncWorkerCompletionRegistry.class);

        manager = new CamelWorkerExecutionManager();
        manager.camelCapabilityResolver = resolver;
        manager.faultPublisher = faultPublisher;
        manager.asyncWorkerCompletionRegistry = registry;
        manager.completionPublisher = mock(WorkflowCompletionPublisher.class);
        manager.producerTemplate = mock(ProducerTemplate.class);
        manager.asyncTimeoutMinutes = 60;
    }

    @Test
    void submit_missingRoute_firesFault() {
        CaseInstance instance = testInstance();
        Worker worker = testWorker();
        Capability cap = Capability.of("missing", "", "");
        when(resolver.resolve(eq("missing"), anyString())).thenThrow(WorkerProvisioningException.noRouteFound("missing"));

        manager.submit(1L, instance, worker, cap, Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L), any(WorkerProvisioningException.class));
    }

    // --- bindingName propagation tests ---

    @Test
    void submit_6arg_passesBindingNameThroughFault() {
        CaseInstance instance = testInstance();
        Worker worker = testWorker();
        Capability cap = Capability.of("missing", "", "");
        when(resolver.resolve(eq("missing"), anyString())).thenThrow(WorkerProvisioningException.noRouteFound("missing"));

        manager.submit(1L, instance, worker, cap, Map.of(), "binding-x");

        org.mockito.ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            org.mockito.ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(faultPublisher).fault(
            ctxCaptor.capture(), eq(cap), eq(1L), any(WorkerProvisioningException.class));
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    @Test
    void submit_5arg_passesNullBindingName() {
        CaseInstance instance = testInstance();
        Worker worker = testWorker();
        Capability cap = Capability.of("missing", "", "");
        when(resolver.resolve(eq("missing"), anyString())).thenThrow(WorkerProvisioningException.noRouteFound("missing"));

        manager.submit(1L, instance, worker, cap, Map.of());

        org.mockito.ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            org.mockito.ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(faultPublisher).fault(
            ctxCaptor.capture(), eq(cap), eq(1L), any(WorkerProvisioningException.class));
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    @Test
    void supports_delegatesToResolver() {
        when(resolver.canResolve("route-1", "t1")).thenReturn(true);
        when(resolver.canResolve("route-2", "t1")).thenReturn(false);

        assertThat(manager.supports("route-1", "t1")).isTrue();
        assertThat(manager.supports("route-2", "t1")).isFalse();
    }

    @Test
    void getActiveWorkCount_delegatesToRegistry() {
        when(registry.countByWorkerName("w1")).thenReturn(3);
        assertThat(manager.getActiveWorkCount("w1")).isEqualTo(3);
    }

    private CaseInstance testInstance() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        return instance;
    }

    private Worker testWorker() {
        return Worker.builder().name("w1").capabilityNames("cap").function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
    }
}
