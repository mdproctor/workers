package io.casehub.workers.scenario;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;

import java.util.Set;
import java.util.logging.Logger;

public class ScenarioWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(ScenarioWorkerRuntime.class.getName());

    private final ScenarioEndpointResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    public ScenarioWorkerRuntime(ScenarioEndpointResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public String workerType() {
        return ScenarioWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public void initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) { return; }
        try {
            if (resolver.endpointNames().isEmpty()) {
                LOG.warning("No scenario endpoints configured — status FAULTED");
                status = WorkerRuntimeStatus.FAULTED;
            } else {
                status = WorkerRuntimeStatus.RUNNING;
            }
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
