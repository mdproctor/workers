package io.casehub.workers.camel;

import io.casehub.workers.common.WorkerRuntimeStatus;
import jakarta.enterprise.inject.Instance;
import org.apache.camel.CamelContext;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CamelWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(createResolver());

        assertThat(runtime.workerType()).isEqualTo("camel");
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
    }

    @Test
    void initialize_transitionsToRunning() {
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(createResolver());

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_populatesCapabilities() {
        CamelCapabilityResolver resolver = createResolver();
        resolver.configCapabilities = Map.of("send-email", "direct:send-email");
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(resolver);

        runtime.initialize();

        assertThat(runtime.capabilities()).contains("send-email");
    }

    @Test
    void shutdown_transitionsToStopped() {
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(createResolver());
        runtime.initialize();

        runtime.shutdown();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }

    @SuppressWarnings("unchecked")
    private CamelCapabilityResolver createResolver() {
        CamelCapabilityResolver resolver = new CamelCapabilityResolver();
        resolver.camelContext = mock(CamelContext.class);
        when(resolver.camelContext.getRoutes()).thenReturn(List.of());

        // Mock empty Instance<CamelWorkerRoute>
        Instance<CamelWorkerRoute> mockInstance = mock(Instance.class);
        when(mockInstance.iterator()).thenReturn(mock(Iterator.class));
        when(mockInstance.iterator().hasNext()).thenReturn(false);
        resolver.spiRoutes = mockInstance;

        resolver.configCapabilities = Map.of();
        return resolver;
    }
}
