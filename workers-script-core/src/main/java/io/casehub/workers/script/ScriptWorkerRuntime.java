package io.casehub.workers.script;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;

import java.util.Set;
import java.util.logging.Logger;

public class ScriptWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(ScriptWorkerRuntime.class.getName());

    private final ScriptDefinitionResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    public ScriptWorkerRuntime(ScriptDefinitionResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public String workerType() {
        return ScriptWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public void initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) { return; }
        try {
            if (resolver.capabilities().isEmpty()) {
                LOG.warning("No scripts configured — status FAULTED");
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
