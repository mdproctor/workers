package io.casehub.workers.common;

import io.casehub.worker.api.Capability;
import java.time.Instant;
import java.util.Map;

public record PendingCompletion(
    String dispatchId,
    String workerType,
    WorkerCorrelationContext correlationContext,
    String callbackToken,
    Capability capability,
    Long eventLogId,
    Instant registeredAt,
    Instant expiresAt,
    Map<String, String> provisionerMeta
) {}
