package io.casehub.workers.common;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class WorkerLifecycleOrchestrator {

    private static final Logger LOG = Logger.getLogger(
        WorkerLifecycleOrchestrator.class.getName());

    private final List<WorkerRuntime> runtimes;
    private final Duration initTimeout;

    public WorkerLifecycleOrchestrator(List<WorkerRuntime> runtimes,
                                       Duration initTimeout) {
        this.runtimes = List.copyOf(runtimes);
        this.initTimeout = initTimeout;
    }

    public void initializeAll() {
        if (runtimes.isEmpty()) {
            LOG.info("No WorkerRuntime beans discovered — no worker modules on classpath");
            return;
        }
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = runtimes.stream()
                .map(rt -> Map.entry(rt, executor.submit(() -> {
                    rt.initialize();
                    return rt;
                })))
                .toList();
            for (var entry : futures) {
                var rt = entry.getKey();
                var future = entry.getValue();
                try {
                    future.get(initTimeout.toMillis(), TimeUnit.MILLISECONDS);
                    if (rt.status() == WorkerRuntimeStatus.RUNNING) {
                        LOG.info(String.format("Worker '%s' initialized — capabilities: %s",
                            rt.workerType(), rt.capabilities()));
                    } else {
                        LOG.warning(String.format(
                            "Worker '%s' did not reach RUNNING after initialize() — status: %s",
                            rt.workerType(), rt.status()));
                    }
                } catch (TimeoutException e) {
                    future.cancel(true);
                    LOG.warning(String.format(
                        "Worker '%s' initialization timed out after %s",
                        rt.workerType(), initTimeout));
                } catch (ExecutionException e) {
                    LOG.warning(String.format("Worker '%s' failed to initialize: %s",
                        rt.workerType(), e.getCause().getMessage()));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdownAll() {
        for (var rt : runtimes) {
            if (rt.status() == WorkerRuntimeStatus.PENDING) { continue; }
            try {
                rt.shutdown();
            } catch (Exception e) {
                LOG.warning(String.format("Worker '%s' shutdown failed: %s",
                    rt.workerType(), e.getMessage()));
            }
        }
    }
}
