package io.casehub.workers.camel;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;

@ApplicationScoped
public class CamelWorkerRuntime implements WorkerRuntime {

    private final CamelCapabilityResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Inject
    CamelWorkerRuntime(CamelCapabilityResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public String workerType() {
        return CamelWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public void initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) { return; }
        try {
            resolver.initialize();
            status = WorkerRuntimeStatus.RUNNING;
        } catch (Exception e) {
            status = WorkerRuntimeStatus.FAULTED;
            throw e;
        }
    }

    @Override
    public void shutdown() {
        status = WorkerRuntimeStatus.STOPPED;
    }

    @Override
    public Set<String> capabilities() {
        return resolver.capabilities();
    }
}
