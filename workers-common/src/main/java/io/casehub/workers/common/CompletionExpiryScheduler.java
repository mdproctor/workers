package io.casehub.workers.common;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CompletionExpiryScheduler {

    @Inject AsyncWorkerCompletionRegistry registry;

    @Scheduled(every = "${casehub.workers.async.expiry-check-interval:5m}")
    @Blocking
    void tick() {
        registry.expireStale();
    }
}
