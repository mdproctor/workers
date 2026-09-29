package io.casehub.workers.script;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.workers.common.WorkerRuntimeStatus;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScriptWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("script");
    }

    @Test
    void initialize_withScripts_transitionsToRunning() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("script:test");
    }

    @Test
    void initialize_noScripts_transitionsToFaulted() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of());
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initialize_whenAlreadyRunning_isNoOp() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize();
        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_whenFaulted_retriesAndRecovers() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of());
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);

        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void shutdown_transitionsToStopped() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize();
        runtime.shutdown();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }
}
