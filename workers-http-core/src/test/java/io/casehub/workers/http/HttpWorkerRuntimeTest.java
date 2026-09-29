package io.casehub.workers.http;

import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointQuery;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.path.Path;
import io.casehub.workers.common.WorkerRuntimeStatus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class HttpWorkerRuntimeTest {

    private static EndpointRegistry emptyRegistry() {
        return new EndpointRegistry() {
            @Override public void register(EndpointDescriptor endpoint) {}
            @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) { return Optional.empty(); }
            @Override public List<EndpointDescriptor> discover(EndpointQuery query) { return List.of(); }
            @Override public void deregister(Path path, String tenancyId) {}
        };
    }

    @Test
    void initialStatus_isPending() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("http");
    }

    @Test
    void initialize_transitionsToRunning() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_delegatesToResolver() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).isEqualTo(resolver.capabilities());
    }

    @Test
    void capabilities_delegatesToResolver() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        resolver.initialize(List.of(), Map.of(
            "send-email", Map.of("url", "https://mail.example.com/send", "method", "POST"),
            "fetch-data", Map.of("url", "https://api.example.com/data", "method", "GET")
        ), 30, emptyRegistry());

        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        assertThat(runtime.capabilities()).containsExactlyInAnyOrder("send-email", "fetch-data");
        assertThat(runtime.capabilities()).isEqualTo(resolver.capabilities());
    }

    @Test
    void initialize_whenAlreadyRunning_isNoOp() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void shutdown_transitionsToStopped() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        runtime.initialize();
        runtime.shutdown();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }

    @Test
    void capabilities_beforeInit_isEmpty() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        assertThat(runtime.capabilities()).isEmpty();
    }
}
