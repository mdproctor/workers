package io.casehub.workers.scenario;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ScenarioWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(ScenarioWorkerRuntime.class);

    private final ScenarioEndpointResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Inject
    ScenarioWorkerRuntime(ScenarioEndpointResolver resolver) {
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
    public Uni<Void> initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) {
            return Uni.createFrom().voidItem();
        }
        return Uni.createFrom().item(() -> {
            try {
                if (resolver.endpointNames().isEmpty()) {
                    resolver.initializeFromConfig();
                }
                if (resolver.endpointNames().isEmpty()) {
                    LOG.warn("No scenario endpoints configured — status FAULTED");
                    status = WorkerRuntimeStatus.FAULTED;
                } else {
                    status = WorkerRuntimeStatus.RUNNING;
                }
            } catch (Exception e) {
                status = WorkerRuntimeStatus.FAULTED;
                throw e;
            }
            return null;
        }).replaceWithVoid();
    }

    @Override
    public Uni<Void> shutdown() {
        status = WorkerRuntimeStatus.STOPPED;
        return Uni.createFrom().voidItem();
    }

    @Override
    public Set<String> capabilities() {
        return resolver.capabilities();
    }
}
