package io.casehub.workers.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.platform.api.governance.RetryPolicy;
import io.casehub.worker.api.Worker;

import java.util.Map;
import java.util.logging.Logger;

public class WorkerFaultHandler {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Logger LOG = Logger.getLogger(WorkerFaultHandler.class.getName());

    private final WorkerRetrySupport retrySupport;
    private final WorkerExecutionManager workerExecutionManager;
    private final EventLogRepository eventLogRepository;

    public WorkerFaultHandler(WorkerRetrySupport retrySupport,
                              WorkerExecutionManager workerExecutionManager,
                              EventLogRepository eventLogRepository) {
        this.retrySupport = retrySupport;
        this.workerExecutionManager = workerExecutionManager;
        this.eventLogRepository = eventLogRepository;
    }

    public void handleFault(WorkerFaultEvent event) {
        CaseInstance instance      = event.caseInstance();
        Worker       worker        = event.worker();
        String       inputDataHash = event.inputDataHash();
        String       tenancyId     = instance.tenancyId;
        String errorMsg = (event.cause() != null && event.cause().getMessage() != null)
                          ? event.cause().getMessage() : "unknown";

        try {
            retrySupport.persistFailureLog(instance, worker, inputDataHash, errorMsg, tenancyId);

            if (event.cause() instanceof PermanentFaultException) {
                retrySupport.publishRetriesExhausted(
                        instance.getUuid(), worker.name(), inputDataHash,
                        event.bindingName(), tenancyId);
                return;
            }

            long failureCount = retrySupport.countFailedAttempts(
                    instance.getUuid(), worker.name(), inputDataHash, tenancyId);
            RetryPolicy retryPolicy = WorkerRetrySupport.resolveRetryPolicy(worker);

            if (failureCount < retryPolicy.maxAttempts()) {
                long delayMs;
                if (event.cause() instanceof RetryAfterException ra) {
                    delayMs = ra.retryAfterMs();
                } else {
                    delayMs = WorkerRetrySupport.computeBackoffDelayMs(
                            retryPolicy, failureCount + 1);
                }
                reloadAndResubmit(event, delayMs);
            } else {
                retrySupport.publishRetriesExhausted(
                        instance.getUuid(), worker.name(), inputDataHash,
                        event.bindingName(), tenancyId);
            }
        } catch (Exception ex) {
            LOG.severe(String.format("Fault handling failed for worker %s case %s — case may stall",
                       worker.name(), instance.getUuid()));
        }
    }

    private void reloadAndResubmit(WorkerFaultEvent event, long delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        EventLog eventLog = eventLogRepository.findById(
                Long.parseLong(event.eventLogId()), event.caseInstance().tenancyId);
        Map<String, Object> inputData =
                OBJECT_MAPPER.convertValue(eventLog.getPayload(), MAP_TYPE);
        workerExecutionManager.submit(
                Long.parseLong(event.eventLogId()),
                event.caseInstance(), event.worker(), event.capability(), inputData,
                event.bindingName());
    }
}
