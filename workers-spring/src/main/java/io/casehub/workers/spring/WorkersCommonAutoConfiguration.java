package io.casehub.workers.spring;

import io.casehub.engine.common.internal.event.WorkerRetriesExhaustedEvent;
import io.casehub.engine.common.internal.event.WorkflowExecutionCompleted;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CompletionExpiredEvent;
import io.casehub.workers.common.WorkerFaultHandler;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerLifecycleOrchestrator;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;

@AutoConfiguration
public class WorkersCommonAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public WorkerRetrySupport workerRetrySupport(EventLogRepository eventLogRepository,
                                                  ApplicationEventPublisher publisher) {
        return new WorkerRetrySupport(eventLogRepository,
            event -> publisher.publishEvent(event));
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerFaultHandler workerFaultHandler(WorkerRetrySupport retrySupport,
                                                  WorkerExecutionManager executionManager,
                                                  EventLogRepository eventLogRepository) {
        return new WorkerFaultHandler(retrySupport, executionManager, eventLogRepository);
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkerFaultPublisher workerFaultPublisher(WorkerFaultHandler handler) {
        return new WorkerFaultPublisher(
            event -> Thread.startVirtualThread(() -> handler.handleFault(event)));
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkflowCompletionPublisher workflowCompletionPublisher(ApplicationEventPublisher publisher) {
        return new WorkflowCompletionPublisher(
            event -> publisher.publishEvent(event));
    }

    @Bean
    @ConditionalOnMissingBean
    public AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry(ApplicationEventPublisher publisher) {
        return new AsyncWorkerCompletionRegistry(
            event -> publisher.publishEvent(event));
    }

    @Bean(destroyMethod = "shutdownAll")
    @ConditionalOnMissingBean
    public WorkerLifecycleOrchestrator workerLifecycleOrchestrator(
            List<WorkerRuntime> runtimes,
            @Value("${casehub.workers.init-timeout-seconds:30}") int timeoutSeconds) {
        WorkerLifecycleOrchestrator orchestrator = new WorkerLifecycleOrchestrator(
            runtimes, Duration.ofSeconds(timeoutSeconds));
        orchestrator.initializeAll();
        return orchestrator;
    }
}
