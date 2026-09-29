package io.casehub.workers.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.event.CaseHubEventType;
import io.casehub.api.model.event.EventStreamType;
import io.casehub.engine.common.internal.event.WorkerRetriesExhaustedEvent;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.platform.api.governance.BackoffStrategy;
import io.casehub.platform.api.governance.ExecutionPolicy;
import io.casehub.platform.api.governance.RetryPolicy;
import io.casehub.worker.api.Worker;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class WorkerRetrySupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter HTTP_DATE =
        DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

    private final EventLogRepository eventLogRepository;
    private final Consumer<WorkerRetriesExhaustedEvent> retriesExhaustedConsumer;

    public WorkerRetrySupport(EventLogRepository eventLogRepository,
                              Consumer<WorkerRetriesExhaustedEvent> retriesExhaustedConsumer) {
        this.eventLogRepository = eventLogRepository;
        this.retriesExhaustedConsumer = retriesExhaustedConsumer;
    }

    public static RetryPolicy resolveRetryPolicy(Worker worker) {
        ExecutionPolicy executionPolicy = worker.executionPolicy();
        if (executionPolicy == null || executionPolicy.retries() == null) {
            return new RetryPolicy();
        }
        return executionPolicy.retries();
    }

    public static long computeBackoffDelayMs(RetryPolicy policy, long attemptNumber) {
        long baseDelayMs = policy.delayMs() != null ? policy.delayMs() : 0L;
        BackoffStrategy strategy = policy.backoffStrategy() != null
            ? policy.backoffStrategy() : BackoffStrategy.FIXED;
        return switch (strategy) {
            case FIXED -> baseDelayMs;
            case EXPONENTIAL -> {
                long shift = Math.min(attemptNumber - 1, 30);
                yield Math.min(baseDelayMs * (1L << shift), 30_000L);
            }
            case EXPONENTIAL_WITH_JITTER -> {
                long shift = Math.min(attemptNumber - 1, 30);
                long cap = Math.min(baseDelayMs * (1L << shift), 30_000L);
                yield cap == 0 ? 0 : ThreadLocalRandom.current().nextLong(cap + 1);
            }
        };
    }

    public static RuntimeException parseRetryAfter(String retryAfter, int status, String statusMessage) {
        String message = status + " " + statusMessage;
        if (retryAfter == null || retryAfter.isBlank()) {
            return new RuntimeException(message);
        }
        try {
            long seconds = Long.parseLong(retryAfter.trim());
            return new RetryAfterException(seconds * 1000, message);
        } catch (NumberFormatException ignored) {
        }
        try {
            ZonedDateTime retryDate = ZonedDateTime.parse(retryAfter.trim(), HTTP_DATE);
            long deltaMs = retryDate.toInstant().toEpochMilli() - System.currentTimeMillis();
            return new RetryAfterException(Math.max(0, deltaMs), message);
        } catch (DateTimeParseException ignored) {
        }
        return new RuntimeException(message);
    }

    public void persistFailureLog(CaseInstance instance, Worker worker,
                                  String inputDataHash, String errorMsg,
                                  String tenancyId) {
        EventLog failureLog = new EventLog();
        failureLog.setCaseId(instance.getUuid());
        failureLog.setWorkerId(worker.name());
        failureLog.setEventType(CaseHubEventType.WORKER_EXECUTION_FAILED);
        failureLog.setStreamType(EventStreamType.CASE);
        failureLog.setTimestamp(Instant.now());
        String msg = (errorMsg != null) ? errorMsg : "unknown";
        failureLog.setMetadata(OBJECT_MAPPER.createObjectNode()
                                            .put("inputDataHash", inputDataHash)
                                            .put("errorMessage", msg));

        eventLogRepository.append(failureLog, tenancyId);
    }

    public long countFailedAttempts(UUID caseId, String workerId,
                                    String inputDataHash, String tenancyId) {
        return eventLogRepository
                       .findByCaseAndWorkerAndType(caseId, workerId, CaseHubEventType.WORKER_EXECUTION_FAILED, tenancyId)
                       .stream()
                       .filter(log -> {
                           JsonNode meta = log.getMetadata();
                           JsonNode node = meta == null ? null : meta.get("inputDataHash");
                           return node != null && inputDataHash.equals(node.asText());
                       })
                       .count();
    }

    public void publishRetriesExhausted(UUID caseId, String workerId, String inputDataHash,
                                        String bindingName, String tenancyId) {
        retriesExhaustedConsumer.accept(
            new WorkerRetriesExhaustedEvent(caseId, tenancyId, workerId, inputDataHash, bindingName, null, null));
    }
}
