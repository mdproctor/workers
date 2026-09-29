package io.casehub.workers.common;

import io.casehub.engine.common.internal.event.EventBusAddresses;
import io.casehub.engine.common.internal.event.WorkerRetriesExhaustedEvent;
import io.casehub.engine.common.internal.event.WorkflowExecutionCompleted;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

@ApplicationScoped
public class WorkersCommonBeans {

    @Inject EventBus eventBus;
    @Inject EventLogRepository eventLogRepository;
    @Inject WorkerExecutionManager workerExecutionManager;

    @Produces @ApplicationScoped
    WorkerRetrySupport retrySupport() {
        return new WorkerRetrySupport(eventLogRepository,
            event -> eventBus.publish(EventBusAddresses.WORKER_RETRIES_EXHAUSTED, event));
    }

    @Produces @ApplicationScoped
    WorkerFaultHandler faultHandler(WorkerRetrySupport retrySupport) {
        return new WorkerFaultHandler(retrySupport, workerExecutionManager,
            eventLogRepository);
    }

    @Produces @ApplicationScoped
    WorkerFaultPublisher faultPublisher(WorkerFaultHandler handler) {
        return new WorkerFaultPublisher(
            event -> Thread.startVirtualThread(() -> handler.handleFault(event)));
    }

    @Produces @ApplicationScoped
    WorkflowCompletionPublisher completionPublisher() {
        return new WorkflowCompletionPublisher(
            event -> eventBus.publish(EventBusAddresses.WORKER_EXECUTION_FINISHED, event));
    }

    @Produces @ApplicationScoped
    AsyncWorkerCompletionRegistry completionRegistry(
            Event<CompletionExpiredEvent> expiryEvents) {
        return new AsyncWorkerCompletionRegistry(
            event -> expiryEvents.fireAsync(event));
    }
}
