package io.casehub.workers.common;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;

import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

@ApplicationScoped
public class QuarkusWorkerLifecycleOrchestrator {

    @Inject @Any
    Instance<WorkerRuntime> runtimes;

    private WorkerLifecycleOrchestrator delegate;

    void onStartup(@Observes @Priority(APPLICATION + 10) StartupEvent ev) {
        if (runtimes == null || runtimes.isUnsatisfied()) { return; }
        delegate = new WorkerLifecycleOrchestrator(
            runtimes.stream().toList(), Duration.ofSeconds(30));
        delegate.initializeAll();
    }

    @PreDestroy
    void onShutdown() {
        if (delegate != null) {
            delegate.shutdownAll();
        }
    }
}
