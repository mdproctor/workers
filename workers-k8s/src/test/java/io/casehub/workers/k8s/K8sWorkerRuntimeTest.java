package io.casehub.workers.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.workers.common.WorkerRuntimeStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class K8sWorkerRuntimeTest {

    K8sWorkerRuntime runtime;
    JobDefinitionResolver resolver;
    K8sJobInformerManager informerManager;
    KubernetesClient client;

    @BeforeEach
    void setUp() {
        resolver = new JobDefinitionResolver("default", 3600, 600, 0, "delete", 1_048_576, 262_144);
        informerManager = mock(K8sJobInformerManager.class);
        client = mock(KubernetesClient.class);
        runtime = new K8sWorkerRuntime();
        runtime.resolver = resolver;
        runtime.informerManager = informerManager;
        runtime.kubernetesClient = client;
    }

    private static JobDefinition imageDef(String name, String namespace) {
        return new JobDefinition(name, namespace, "img:v1",
            List.of(), List.of(), null, null, null, null, null,
            3600, 600, 0, 1_048_576, null, Map.of(), Map.of(), CleanupPolicy.DELETE);
    }

    @Test
    void initialStatus_isPending() {
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("k8s");
    }

    @Test
    void initialize_withJobs_transitionsToRunning() {
        resolver.initialize(Map.of("test", imageDef("test", "batch")));
        when(informerManager.hasActiveInformers()).thenReturn(true);

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("k8s:test");
        verify(informerManager).start(Set.of("batch"));
    }

    @Test
    void initialize_noJobs_transitionsToFaulted() {
        resolver.initialize(Map.of());

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initialize_allInformersFailed_transitionsToFaulted() {
        resolver.initialize(Map.of("test", imageDef("test", "batch")));
        when(informerManager.hasActiveInformers()).thenReturn(false);

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initialize_whenAlreadyRunning_isNoOp() {
        resolver.initialize(Map.of("test", imageDef("test", "batch")));
        when(informerManager.hasActiveInformers()).thenReturn(true);

        runtime.initialize();
        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_whenFaulted_retriesAndRecovers() {
        resolver.initialize(Map.of());
        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);

        resolver.initialize(Map.of("test", imageDef("test", "batch")));
        when(informerManager.hasActiveInformers()).thenReturn(true);
        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void shutdown_transitionsToStopped() {
        resolver.initialize(Map.of("test", imageDef("test", "batch")));
        when(informerManager.hasActiveInformers()).thenReturn(true);

        runtime.initialize();
        runtime.shutdown();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
        verify(informerManager).stop();
    }
}
