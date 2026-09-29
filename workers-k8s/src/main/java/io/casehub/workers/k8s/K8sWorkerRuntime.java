package io.casehub.workers.k8s;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class K8sWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(K8sWorkerRuntime.class);

    @Inject JobDefinitionResolver resolver;
    @Inject K8sJobInformerManager informerManager;
    @Inject KubernetesClient kubernetesClient;

    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Override
    public String workerType() {
        return K8sWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public void initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) { return; }
        if (resolver.capabilities().isEmpty()) {
            status = WorkerRuntimeStatus.FAULTED;
            LOG.warn("No K8s job definitions configured — runtime FAULTED");
            return;
        }
        try {
            kubernetesClient.getApiVersion();
        } catch (Exception e) {
            status = WorkerRuntimeStatus.FAULTED;
            LOG.warnf("K8s cluster unreachable: %s — runtime FAULTED", e.getMessage());
            return;
        }
        Set<String> namespaces = resolver.namespaces();
        informerManager.start(namespaces);
        if (!informerManager.hasActiveInformers()) {
            status = WorkerRuntimeStatus.FAULTED;
            LOG.warn("All namespace informers failed — runtime FAULTED");
            return;
        }
        status = WorkerRuntimeStatus.RUNNING;
    }

    @Override
    public void shutdown() {
        informerManager.stop();
        status = WorkerRuntimeStatus.STOPPED;
    }

    @Override
    public Set<String> capabilities() {
        return resolver.capabilities();
    }
}
