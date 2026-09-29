package io.casehub.workers.http;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;

import java.util.Set;

public class HttpWorkerRuntime implements WorkerRuntime {

    private final HttpEndpointResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    public HttpWorkerRuntime(HttpEndpointResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public String workerType() {
        return HttpWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public void initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) { return; }
        try {
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
