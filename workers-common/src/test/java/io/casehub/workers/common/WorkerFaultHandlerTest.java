package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.platform.api.governance.BackoffStrategy;
import io.casehub.worker.api.Capability;
import io.casehub.platform.api.governance.ExecutionPolicy;
import io.casehub.platform.api.governance.RetryPolicy;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import io.casehub.workers.common.WorkerFaultEvent;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class WorkerFaultHandlerTest {

    private WorkerFaultHandler handler;
    private WorkerRetrySupport retrySupport;
    private WorkerExecutionManager workerExecutionManager;
    private EventLogRepository eventLogRepository;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() {
        retrySupport = mock(WorkerRetrySupport.class);
        workerExecutionManager = mock(WorkerExecutionManager.class);
        eventLogRepository = mock(EventLogRepository.class);

        handler = new WorkerFaultHandler(retrySupport, workerExecutionManager, eventLogRepository);
    }

    @Test
    void permanentFault_skipsRetry() {
        CaseInstance instance = testCaseInstance();
        Worker worker = testWorker("w1");
        Capability cap = testCapability("cap");
        PermanentFaultException cause = new PermanentFaultException(400, "Bad Request");

        WorkerFaultEvent event = new WorkerFaultEvent(
            instance, worker, cap, "hash-1", "42", cause, null);

        handler.handleFault(event);

        verify(retrySupport).persistFailureLog(instance, worker, "hash-1", "Bad Request", instance.tenancyId);
        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1",
            null, instance.tenancyId);
        verify(retrySupport, never()).countFailedAttempts(any(), any(), any(), any());
        verify(workerExecutionManager, never()).submit(anyLong(), any(), any(), any(), any());
    }

    @Test
    void retryableFault_underMaxRetries_callsSubmit() {
        CaseInstance instance = testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 100, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = Worker.builder().name("w1").capabilityNames().executionPolicy(ep)
            .function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        Capability cap = testCapability("cap");
        RuntimeException cause = new RuntimeException("transient error");

        // First failure — count=1, maxAttempts=3, so 1 < 3 → retry
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(1L);

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("key", "value");
        eventLog.setPayload(payload);
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(eventLog);

        WorkerFaultEvent event = new WorkerFaultEvent(
            instance, worker, cap, "hash-1", "42", cause, null);

        handler.handleFault(event);

        // Verify submit was called with the reloaded input data
        ArgumentCaptor<Map<String, Object>> inputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(workerExecutionManager).submit(
            eq(42L), eq(instance), eq(worker), eq(cap), inputCaptor.capture(), eq(null));
        assertThat(inputCaptor.getValue()).containsEntry("key", "value");

        // Verify publishRetriesExhausted was NOT called
        verify(retrySupport, never()).publishRetriesExhausted(any(), any(), any(), any(), any());
    }

    @Test
    void retriesExhausted_neverCallsSubmit() {
        CaseInstance instance = testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = Worker.builder().name("w1").capabilityNames().executionPolicy(ep)
            .function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        Capability cap = testCapability("cap");
        RuntimeException cause = new RuntimeException("500 error");

        // failureCount == maxAttempts → strict < means exhausted
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(3L);

        WorkerFaultEvent event = new WorkerFaultEvent(
            instance, worker, cap, "hash-1", "42", cause, null);

        handler.handleFault(event);

        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1",
            null, instance.tenancyId);
        verify(workerExecutionManager, never()).submit(anyLong(), any(), any(), any(), any());
    }

    @Test
    void retryAfterException_usesRetryAfterDelay() {
        CaseInstance instance = testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = Worker.builder().name("w1").capabilityNames().executionPolicy(ep)
            .function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        Capability cap = testCapability("cap");
        RetryAfterException cause = new RetryAfterException(200, "429 Too Many Requests");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(1L);

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("key", "value");
        eventLog.setPayload(payload);
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(eventLog);

        WorkerFaultEvent event = new WorkerFaultEvent(
            instance, worker, cap, "hash-1", "42", cause, null);

        // With a real Vertx timer, the delay of 200ms should complete
        handler.handleFault(event);

        // Should have called submit (RetryAfter overrides configured backoff of 10000ms)
        verify(workerExecutionManager).submit(eq(42L), eq(instance), eq(worker), eq(cap), any(), eq(null));
        verify(retrySupport, never()).publishRetriesExhausted(any(), any(), any(), any(), any());
    }

    @Test
    void retryableFault_passesBindingNameToSubmit() {
        CaseInstance instance = testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 100, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = Worker.builder().name("w1").capabilityNames().executionPolicy(ep)
            .function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        Capability cap = testCapability("cap");
        RuntimeException cause = new RuntimeException("transient");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(1L);

        EventLog eventLog = new EventLog();
        eventLog.setPayload(OBJECT_MAPPER.createObjectNode().put("k", "v"));
        when(eventLogRepository.findById(42L, instance.tenancyId)).thenReturn(eventLog);
        WorkerFaultEvent event = new WorkerFaultEvent(
            instance, worker, cap, "hash-1", "42", cause, "binding-x");

        handler.handleFault(event);

        verify(workerExecutionManager).submit(
            eq(42L), eq(instance), eq(worker), eq(cap), any(), eq("binding-x"));
    }

    @Test
    void permanentFault_passesBindingNameToRetriesExhausted() {
        CaseInstance instance = testCaseInstance();
        Worker worker = testWorker("w1");
        Capability cap = testCapability("cap");
        PermanentFaultException cause = new PermanentFaultException(400, "Bad Request");

        WorkerFaultEvent event = new WorkerFaultEvent(
            instance, worker, cap, "hash-1", "42", cause, "binding-y");

        handler.handleFault(event);

        verify(retrySupport).publishRetriesExhausted(
            instance.getUuid(), "w1", "hash-1", "binding-y", instance.tenancyId);
    }

    @Test
    void nullBindingName_passesNullToRetriesExhausted() {
        CaseInstance instance = testCaseInstance();
        Worker worker = testWorker("w1");
        Capability cap = testCapability("cap");
        PermanentFaultException cause = new PermanentFaultException(400, "Bad");

        WorkerFaultEvent event = new WorkerFaultEvent(
            instance, worker, cap, "hash-1", "42", cause, null);

        handler.handleFault(event);

        verify(retrySupport).publishRetriesExhausted(
            instance.getUuid(), "w1", "hash-1", null, instance.tenancyId);
    }

    // ── Helpers ──

    private static CaseInstance testCaseInstance() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "test-tenant";
        return instance;
    }

    private static Worker testWorker(String name) {
        return Worker.builder().name(name).capabilityNames().function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
    }

    private static Capability testCapability(String tag) {
        return Capability.of(tag, "", "");
    }
}
