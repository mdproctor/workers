package io.casehub.workers.script;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.testing.WorkerTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ScriptWorkerExecutionManagerTest {

    ScriptWorkerExecutionManager manager;
    ScriptDefinitionResolver resolver;
    WorkerFaultPublisher faultPublisher;
    WorkflowCompletionPublisher completionPublisher;

    @BeforeEach
    void setUp() {
        resolver = new ScriptDefinitionResolver(300, 1_048_576);
        faultPublisher = mock(WorkerFaultPublisher.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        manager = new ScriptWorkerExecutionManager(resolver, faultPublisher, completionPublisher);
    }

    @Test
    void submit_jsonObjectStdout_parsedAsStructuredOutput() {
        resolver.initialize(Map.of(
            "json-script", new ScriptDefinition("json-script", "/bin/sh",
                List.of("-c", "echo '{\"result\":\"ok\"}'"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:json-script"),
            WorkerTestSupport.testCapability("script:json-script"),
            Map.of());

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        assertThat(captor.getValue()).containsEntry("result", "ok");
    }

    @Test
    void submit_nonJsonStdout_wrappedAsRaw() {
        resolver.initialize(Map.of(
            "text-script", new ScriptDefinition("text-script", "/bin/sh",
                List.of("-c", "echo hello world"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:text-script"),
            WorkerTestSupport.testCapability("script:text-script"),
            Map.of());

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        Map<String, Object> output = captor.getValue();
        assertThat(output.get("stdout").toString()).contains("hello world");
        assertThat(output).containsKey("stderr");
        assertThat(output).containsEntry("exitCode", 0);
    }

    @Test
    void submit_jsonArrayStdout_fallsThroughToRawWrapper() {
        resolver.initialize(Map.of(
            "array-script", new ScriptDefinition("array-script", "/bin/sh",
                List.of("-c", "echo '[1,2,3]'"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:array-script"),
            WorkerTestSupport.testCapability("script:array-script"),
            Map.of());

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        Map<String, Object> output = captor.getValue();
        assertThat(output.get("stdout").toString()).contains("[1,2,3]");
        assertThat(output).containsEntry("exitCode", 0);
    }

    @Test
    void submit_nonZeroExit_faults() {
        resolver.initialize(Map.of(
            "fail-script", new ScriptDefinition("fail-script", "/bin/sh",
                List.of("-c", "exit 1"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:fail-script"),
            WorkerTestSupport.testCapability("script:fail-script"),
            Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(RuntimeException.class));
        verify(completionPublisher, never()).complete(any(), any());
    }

    @Test
    void submit_timeout_permanentFault() {
        resolver.initialize(Map.of(
            "slow-script", new ScriptDefinition("slow-script", "/bin/sh",
                List.of("-c", "sleep 60"),
                null, Map.of(), 1, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:slow-script"),
            WorkerTestSupport.testCapability("script:slow-script"),
            Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_stdinDelivery_inputDataReachesProcess() {
        resolver.initialize(Map.of(
            "stdin-script", new ScriptDefinition("stdin-script", "/bin/sh",
                List.of("-c", "cat"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:stdin-script"),
            WorkerTestSupport.testCapability("script:stdin-script"),
            Map.of("key", "value"));

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        assertThat(captor.getValue()).containsEntry("key", "value");
    }

    @Test
    void submit_environmentVariablesSet() {
        resolver.initialize(Map.of(
            "env-script", new ScriptDefinition("env-script", "/bin/sh",
                List.of("-c", "echo $CASEHUB_CAPABILITY"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:env-script"),
            WorkerTestSupport.testCapability("script:env-script"),
            Map.of());

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        assertThat(captor.getValue().get("stdout").toString()).contains("script:env-script");
    }

    @Test
    void submit_commandNotFound_permanentFault() {
        resolver.initialize(Map.of(
            "missing-cmd", new ScriptDefinition("missing-cmd",
                "/nonexistent/command/xyz123",
                List.of(), null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:missing-cmd"),
            WorkerTestSupport.testCapability("script:missing-cmd"),
            Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_unknownCapability_permanentFault() {
        resolver.initialize(Map.of());

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:missing"),
            WorkerTestSupport.testCapability("script:missing"),
            Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    // --- bindingName propagation tests ---

    @Test
    void submit_6arg_passesBindingNameThroughCompletion() {
        resolver.initialize(Map.of(
            "echo-script", new ScriptDefinition("echo-script", "/bin/sh",
                List.of("-c", "echo '{\"result\":\"ok\"}'"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:echo-script"),
            WorkerTestSupport.testCapability("script:echo-script"),
            Map.of(), "binding-x");

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionPublisher).complete(ctxCaptor.capture(), any());
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    @Test
    void submit_5arg_passesNullBindingName() {
        resolver.initialize(Map.of(
            "echo-script2", new ScriptDefinition("echo-script2", "/bin/sh",
                List.of("-c", "echo '{\"result\":\"ok\"}'"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:echo-script2"),
            WorkerTestSupport.testCapability("script:echo-script2"),
            Map.of());

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionPublisher).complete(ctxCaptor.capture(), any());
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    @Test
    void supports_delegatesToResolver() {
        resolver.initialize(Map.of(
            "script-1", new ScriptDefinition("script-1", "/bin/sh", List.of("-c", "echo ok"), null, Map.of(), 30, 1_048_576)
        ));

        assertThat(manager.supports("script:script-1", "t1")).isTrue();
        assertThat(manager.supports("script:script-2", "t1")).isFalse();
    }
}
