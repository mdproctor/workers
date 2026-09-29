package io.casehub.workers.common;

import com.fasterxml.jackson.databind.JsonNode;
import io.casehub.api.model.event.CaseHubEventType;
import io.casehub.engine.common.internal.event.WorkerRetriesExhaustedEvent;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.platform.api.governance.BackoffStrategy;
import io.casehub.platform.api.governance.ExecutionPolicy;
import io.casehub.platform.api.governance.RetryPolicy;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WorkerRetrySupportTest {

    // ── Static method tests — pure logic, no CDI ──

    @Nested
    class ResolveRetryPolicy {

        @Test
        void nullExecutionPolicy_returnsDefault() {
            Worker worker = testWorker(null);
            RetryPolicy result = WorkerRetrySupport.resolveRetryPolicy(worker);

            assertThat(result.maxAttempts()).isEqualTo(3);
            assertThat(result.delayMs()).isEqualTo(10000);
            assertThat(result.backoffStrategy()).isEqualTo(BackoffStrategy.FIXED);
        }

        @Test
        void nullRetries_returnsDefault() {
            Worker worker = testWorker(new ExecutionPolicy(5000, null));
            RetryPolicy result = WorkerRetrySupport.resolveRetryPolicy(worker);

            assertThat(result.maxAttempts()).isEqualTo(3);
            assertThat(result.delayMs()).isEqualTo(10000);
            assertThat(result.backoffStrategy()).isEqualTo(BackoffStrategy.FIXED);
        }

        @Test
        void explicitPolicy_returnsUnchanged() {
            RetryPolicy explicit = new RetryPolicy(5, 2000, BackoffStrategy.EXPONENTIAL);
            Worker worker = testWorker(new ExecutionPolicy(5000, explicit));
            RetryPolicy result = WorkerRetrySupport.resolveRetryPolicy(worker);

            assertThat(result).isSameAs(explicit);
        }

        @Test
        void workerWithDefaultExecutionPolicy_returnsItsRetryPolicy() {
            // Worker's default ExecutionPolicy() has new RetryPolicy() — should return it
            Worker worker = Worker.builder().name("w1").capabilityNames().function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
            // default ExecutionPolicy sets retries = new RetryPolicy()
            RetryPolicy result = WorkerRetrySupport.resolveRetryPolicy(worker);

            assertThat(result.maxAttempts()).isEqualTo(3);
            assertThat(result.delayMs()).isEqualTo(10000);
            assertThat(result.backoffStrategy()).isEqualTo(BackoffStrategy.FIXED);
        }
    }

    @Nested
    class ComputeBackoffDelayMs {

        @Test
        void fixed_returnsBaseDelayRegardlessOfAttempt() {
            RetryPolicy policy = new RetryPolicy(3, 5000, BackoffStrategy.FIXED);

            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 1)).isEqualTo(5000L);
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 2)).isEqualTo(5000L);
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 10)).isEqualTo(5000L);
        }

        @Test
        void exponential_doublesEachAttempt() {
            RetryPolicy policy = new RetryPolicy(5, 1000, BackoffStrategy.EXPONENTIAL);

            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 1)).isEqualTo(1000L);  // 1000 * 2^0
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 2)).isEqualTo(2000L);  // 1000 * 2^1
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 3)).isEqualTo(4000L);  // 1000 * 2^2
        }

        @Test
        void exponential_capsAt30Seconds() {
            RetryPolicy policy = new RetryPolicy(10, 10000, BackoffStrategy.EXPONENTIAL);

            // attempt 3: 10000 * 4 = 40000 → capped at 30000
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 3)).isEqualTo(30_000L);
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 10)).isEqualTo(30_000L);
        }

        @Test
        void exponentialWithJitter_inRange() {
            RetryPolicy policy = new RetryPolicy(3, 5000, BackoffStrategy.EXPONENTIAL_WITH_JITTER);

            // attempt 1: cap = 5000 * 2^0 = 5000 → result in [0, 5000]
            for (int i = 0; i < 100; i++) {
                long delay = WorkerRetrySupport.computeBackoffDelayMs(policy, 1);
                assertThat(delay).isBetween(0L, 5000L);
            }
        }

        @Test
        void exponentialWithJitter_capsAt30Seconds() {
            RetryPolicy policy = new RetryPolicy(10, 20000, BackoffStrategy.EXPONENTIAL_WITH_JITTER);

            // attempt 5: 20000 * 2^4 = 320000 → capped at 30000 → jitter in [0, 30000]
            for (int i = 0; i < 100; i++) {
                long delay = WorkerRetrySupport.computeBackoffDelayMs(policy, 5);
                assertThat(delay).isBetween(0L, 30_000L);
            }
        }

        @Test
        void nullDelayMs_treatedAsZero() {
            RetryPolicy policy = new RetryPolicy(3, null, BackoffStrategy.FIXED);
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 1)).isEqualTo(0L);
        }

        @Test
        void nullBackoffStrategy_defaultsToFixed() {
            RetryPolicy policy = new RetryPolicy(3, 5000, null);
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 1)).isEqualTo(5000L);
            assertThat(WorkerRetrySupport.computeBackoffDelayMs(policy, 2)).isEqualTo(5000L);
        }
    }

    // ── Instance method tests — mock EventLogRepository ──

    @Nested
    class PersistFailureLog {

        private EventLogRepository eventLogRepository;
        private WorkerRetrySupport support;

        @BeforeEach
        void setUp() {
            eventLogRepository = mock(EventLogRepository.class);
            @SuppressWarnings("unchecked") Consumer<WorkerRetriesExhaustedEvent> consumer = mock(Consumer.class);
            support = new WorkerRetrySupport(eventLogRepository, consumer);
        }

        @Test
        void constructsCorrectEventLogAndCallsAppend() {
            CaseInstance instance = testCaseInstance();
            Worker worker = Worker.builder().name("send-email").capabilityNames().function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
            support.persistFailureLog(instance, worker, "hash-abc", "Connection refused", "tenant-1");

            ArgumentCaptor<EventLog> captor = ArgumentCaptor.forClass(EventLog.class);
            verify(eventLogRepository).append(captor.capture(), eq("tenant-1"));

            EventLog log = captor.getValue();
            assertThat(log.getCaseId()).isEqualTo(instance.getUuid());
            assertThat(log.getWorkerId()).isEqualTo(worker.name());
            assertThat(log.getEventType()).isEqualTo(CaseHubEventType.WORKER_EXECUTION_FAILED);
            assertThat(log.getTimestamp()).isNotNull();

            JsonNode meta = log.getMetadata();
            assertThat(meta.get("inputDataHash").asText()).isEqualTo("hash-abc");
            assertThat(meta.get("errorMessage").asText()).isEqualTo("Connection refused");
        }

        @Test
        void nullErrorMessage_usesUnknown() {
            CaseInstance instance = testCaseInstance();
            Worker worker = Worker.builder().name("send-email").capabilityNames().function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
            support.persistFailureLog(instance, worker, "hash-abc", null, "tenant-1");

            ArgumentCaptor<EventLog> captor = ArgumentCaptor.forClass(EventLog.class);
            verify(eventLogRepository).append(captor.capture(), eq("tenant-1"));
            assertThat(captor.getValue().getMetadata().get("errorMessage").asText()).isEqualTo("unknown");
        }
    }

    @Nested
    class CountFailedAttempts {

        private EventLogRepository eventLogRepository;
        private WorkerRetrySupport support;

        @BeforeEach
        void setUp() {
            eventLogRepository = mock(EventLogRepository.class);
            @SuppressWarnings("unchecked") Consumer<WorkerRetriesExhaustedEvent> consumer = mock(Consumer.class);
            support = new WorkerRetrySupport(eventLogRepository, consumer);
        }

        @Test
        void filtersByInputDataHash() {
            UUID caseId = UUID.randomUUID();

            // Two logs: one matches hash, one doesn't
            EventLog matching = new EventLog();
            matching.setMetadata(new com.fasterxml.jackson.databind.ObjectMapper()
                .createObjectNode().put("inputDataHash", "hash-1"));

            EventLog nonMatching = new EventLog();
            nonMatching.setMetadata(new com.fasterxml.jackson.databind.ObjectMapper()
                .createObjectNode().put("inputDataHash", "hash-OTHER"));

            EventLog noMeta = new EventLog();
            // metadata is null

            when(eventLogRepository.findByCaseAndWorkerAndType(
                    caseId, "w1", CaseHubEventType.WORKER_EXECUTION_FAILED, "t1"))
                .thenReturn(List.of(matching, nonMatching, noMeta));

            long count = support.countFailedAttempts(caseId, "w1", "hash-1", "t1");

            assertThat(count).isEqualTo(1L);
        }

        @Test
        void emptyLogs_returnsZero() {
            UUID caseId = UUID.randomUUID();
            when(eventLogRepository.findByCaseAndWorkerAndType(
                    caseId, "w1", CaseHubEventType.WORKER_EXECUTION_FAILED, "t1"))
                .thenReturn(List.of());

            long count = support.countFailedAttempts(caseId, "w1", "hash-1", "t1");

            assertThat(count).isEqualTo(0L);
        }
    }

    @Nested
    class PublishRetriesExhausted {

        @Test
        void publishesToCorrectAddressWithCorrectEvent() {
            @SuppressWarnings("unchecked") Consumer<WorkerRetriesExhaustedEvent> consumer = mock(Consumer.class);
            WorkerRetrySupport support = new WorkerRetrySupport(mock(EventLogRepository.class), consumer);

            UUID caseId = UUID.randomUUID();
            support.publishRetriesExhausted(caseId, "send-email", "hash-xyz",
                "send-email", "tenant-1");

            ArgumentCaptor<WorkerRetriesExhaustedEvent> captor =
                ArgumentCaptor.forClass(WorkerRetriesExhaustedEvent.class);
            verify(consumer).accept(captor.capture());

            WorkerRetriesExhaustedEvent event = captor.getValue();
            assertThat(event.caseId()).isEqualTo(caseId);
            assertThat(event.workerId()).isEqualTo("send-email");
            assertThat(event.idempotency()).isEqualTo("hash-xyz");
            assertThat(event.bindingName()).isEqualTo("send-email");
            assertThat(event.tenancyId()).isEqualTo("tenant-1");
        }
    }

    // ── Helpers ──

    private static Worker testWorker(ExecutionPolicy policy) {
        Worker.Builder builder = Worker.builder()
            .name("test-worker")
            .capabilityNames("cap")
            .function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of())));
        if (policy != null) {
            builder.executionPolicy(policy);
        }
        return builder.build();
    }

    private static CaseInstance testCaseInstance() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "test-tenant";
        return instance;
    }
}
