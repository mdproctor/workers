package io.casehub.workers.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.workers.common.WorkerRuntimeStatus;
import java.util.List;
import org.junit.jupiter.api.Test;

class ScenarioWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        ScenarioWorkerRuntime runtime = new ScenarioWorkerRuntime(resolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("scenario");
    }

    @Test
    void initialize_withEndpoints_transitionsToRunning() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);
        ScenarioWorkerRuntime runtime = new ScenarioWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("scenario:onboard");
    }

    @Test
    void initialize_noEndpoints_transitionsToFaulted() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300);
        ScenarioWorkerRuntime runtime = new ScenarioWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initialize_whenAlreadyRunning_isNoOp() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);
        ScenarioWorkerRuntime runtime = new ScenarioWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_whenFaulted_retriesAndRecovers() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300);
        ScenarioWorkerRuntime runtime = new ScenarioWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);

        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void shutdown_transitionsToStopped() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);
        ScenarioWorkerRuntime runtime = new ScenarioWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        runtime.shutdown().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }
}
