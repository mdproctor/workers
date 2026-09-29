package io.casehub.workers.common;

import io.casehub.worker.api.Capability;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class AsyncWorkerCompletionRegistry {

    private final Consumer<CompletionExpiredEvent> expiryConsumer;
    private final ConcurrentHashMap<String, PendingCompletion> pending = new ConcurrentHashMap<>();

    public AsyncWorkerCompletionRegistry(Consumer<CompletionExpiredEvent> expiryConsumer) {
        this.expiryConsumer = expiryConsumer;
    }

    public PendingCompletion register(String workerType,
                                      WorkerCorrelationContext ctx,
                                      Capability capability, Long eventLogId,
                                      Duration ttl, Map<String, String> provisionerMeta) {
        Instant now = Instant.now();
        PendingCompletion entry = new PendingCompletion(
            UUID.randomUUID().toString(),
            workerType,
            ctx,
            UUID.randomUUID().toString(),
            capability,
            eventLogId,
            now,
            now.plus(ttl),
            provisionerMeta);
        pending.put(entry.dispatchId(), entry);
        return entry;
    }

    public Optional<PendingCompletion> complete(String dispatchId) {
        return Optional.ofNullable(pending.remove(dispatchId));
    }

    public int countByWorkerName(String workerName) {
        return (int) pending.values().stream()
            .filter(p -> p.correlationContext().worker().name().equals(workerName))
            .count();
    }

    public void expireStale() {
        pending.forEach((key, value) ->
            pending.computeIfPresent(key, (k, p) -> {
                if (!p.expiresAt().isBefore(Instant.now())) return p;
                expiryConsumer.accept(new CompletionExpiredEvent(p));
                return null;
            })
        );
    }
}
