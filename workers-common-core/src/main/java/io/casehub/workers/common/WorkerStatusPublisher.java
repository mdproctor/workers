package io.casehub.workers.common;

import io.casehub.api.model.WorkResult;
import io.casehub.api.spi.WorkerStatusListener;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Map;

@ApplicationScoped
public class WorkerStatusPublisher {

    @Inject
    WorkerStatusListener workerStatusListener;

    public void onWorkerStarted(String dispatchId, Map<String, String> sessionMeta) {
        workerStatusListener.onWorkerStarted(dispatchId, sessionMeta);
    }

    public void onWorkerCompleted(String dispatchId, WorkResult result) {
        workerStatusListener.onWorkerCompleted(dispatchId, result);
    }

    public void onWorkerStalled(String dispatchId) {
        workerStatusListener.onWorkerStalled(dispatchId);
    }
}
