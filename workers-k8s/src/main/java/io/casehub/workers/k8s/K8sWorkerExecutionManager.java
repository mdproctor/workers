package io.casehub.workers.k8s;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.CaseInstanceRepository;
import io.casehub.engine.common.spi.scheduler.WorkerBackend;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

@WorkerBackend
@Priority(10)
@ApplicationScoped
public class K8sWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(K8sWorkerExecutionManager.class);

    @Inject JobDefinitionResolver resolver;
    @Inject AsyncWorkerCompletionRegistry registry;
    @Inject WorkerFaultPublisher faultPublisher;
    @Inject KubernetesClient kubernetesClient;
    @Inject K8sJobInformerManager informerManager;
    @Inject CaseInstanceRepository caseInstanceRepository;
    @Inject ObjectMapper objectMapper;

    @ConfigProperty(name = "casehub.workers.k8s.max-input-bytes", defaultValue = "262144")
    long maxInputBytes;

    @Override
    public boolean supports(String capabilityName, String tenancyId) {
        return resolver.canResolve(capabilityName, tenancyId);
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return registry.countByWorkerName(workerId);
    }

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData) {
        submit(eventLogId, instance, worker, capability, inputData, null);
    }

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData,
                       String bindingName) {
        JobDefinition definition;
        try {
            definition = resolver.resolve(capability.name(), instance.tenancyId);
        } catch (WorkerProvisioningException e) {
            WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);
            faultPublisher.fault(K8sWorkerEventBusAddresses.K8S_WORKER_FAULT,
                                 ctx, capability, eventLogId, new PermanentFaultException(0, e.getMessage()));
            return;
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);

        String inputDataJson;
        try {
            inputDataJson = objectMapper.writeValueAsString(inputData);
        } catch (Exception e) {
            faultPublisher.fault(K8sWorkerEventBusAddresses.K8S_WORKER_FAULT,
                                 ctx, capability, eventLogId, new PermanentFaultException(0,
                                                                                          "Failed to serialize inputData: " + e.getMessage()));
            return;
        }

        if (inputDataJson.getBytes().length > maxInputBytes) {
            faultPublisher.fault(K8sWorkerEventBusAddresses.K8S_WORKER_FAULT,
                                 ctx, capability, eventLogId, new PermanentFaultException(0,
                                                                                          "Input data (" + inputDataJson.getBytes().length
                                                                                          + " bytes) exceeds maxInputBytes limit (" + maxInputBytes + ")"));
            return;
        }

        try {
            PendingCompletion pending = registry.register(
                    K8sWorkerConstants.WORKER_TYPE,
                    K8sWorkerEventBusAddresses.K8S_WORKER_FAULT,
                    ctx, capability, eventLogId,
                    Duration.ofSeconds(definition.timeoutSeconds() + 300),
                    Map.of(
                            "cleanup", definition.cleanup().name(),
                            "maxOutputBytes", String.valueOf(definition.maxOutputBytes())
                          ));

            try {
                Job job = K8sJobBuilder.build(definition, pending.dispatchId(),
                                              instance.getUuid().toString(), instance.tenancyId,
                                              capability.name(), ctx.idempotency(), inputDataJson,
                                              worker.name(), eventLogId, ctx.bindingName());
                kubernetesClient.resource(job).create();
            } catch (KubernetesClientException e) {
                registry.complete(pending.dispatchId());
                throw classifyApiServerError(e);
            } catch (Exception e) {
                registry.complete(pending.dispatchId());
                throw e;
            }
        } catch (Exception t) {
            faultPublisher.fault(K8sWorkerEventBusAddresses.K8S_WORKER_FAULT,
                                 ctx, capability, eventLogId, t);
        }
    }

    private RuntimeException classifyApiServerError(KubernetesClientException e) {
        int code = e.getCode();
        if (code == 403) return new PermanentFaultException(403, "Forbidden: " + e.getMessage());
        if (code == 404) return new PermanentFaultException(404, "Namespace not found: " + e.getMessage());
        if (code == 422) return new PermanentFaultException(422, "Invalid Job spec: " + e.getMessage());
        return new RuntimeException(e.getMessage(), e);
    }

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData,
                                              String bindingName) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.name(), capability.name(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId, bindingName);
    }

    @Override
    public void schedulePersistedEvent(EventLog scheduledEventLog) {
        if (scheduledEventLog.getMetadata() == null) {
            return;
        }
        String capabilityName = scheduledEventLog.getMetadata().has("capabilityName")
                                ? scheduledEventLog.getMetadata().get("capabilityName").asText() : null;
        String workerName = scheduledEventLog.getMetadata().has("workerName")
                            ? scheduledEventLog.getMetadata().get("workerName").asText() : null;
        String bindingName = scheduledEventLog.getMetadata().has("bindingName")
                             ? scheduledEventLog.getMetadata().get("bindingName").asText() : null;

        if (capabilityName == null || workerName == null) {
            LOG.warnf("schedulePersistedEvent: missing metadata — capabilityName=%s workerName=%s",
                      capabilityName, workerName);
            return;
        }

        JobDefinition definition;
        try {
            definition = resolver.resolve(capabilityName, scheduledEventLog.tenancyId);
        } catch (Exception e) {
            LOG.warnf("schedulePersistedEvent: capability '%s' not resolvable — config removed?",
                      capabilityName);
            return;
        }

        UUID   caseId     = scheduledEventLog.getCaseId();
        String tenancyId  = scheduledEventLog.tenancyId;
        Long   eventLogId = scheduledEventLog.id;

        Map<String, String> labelSelector = Map.of(
                K8sWorkerConstants.MANAGED_BY_LABEL, K8sWorkerConstants.MANAGED_BY_VALUE,
                K8sWorkerConstants.CASE_ID_LABEL, caseId.toString(),
                K8sWorkerConstants.CAPABILITY_LABEL, capabilityName,
                K8sWorkerConstants.WORKER_NAME_LABEL, workerName);

        for (String ns : resolver.namespaces()) {
            var existing = kubernetesClient.resources(Job.class)
                                           .inNamespace(ns).withLabels(labelSelector).list();
            if (!existing.getItems().isEmpty()) {
                LOG.infof("schedulePersistedEvent: existing Job found for case %s capability %s — informer handles it",
                          caseId, capabilityName);
                return;
            }
        }

        CaseInstance instance = caseInstanceRepository.findByUuid(caseId, tenancyId);
        if (instance == null) {
            LOG.warnf("schedulePersistedEvent: CaseInstance %s not found — case closed?", caseId);
            return;
        }
        Worker worker = Worker.builder()
                              .name(workerName)
                              .capabilityName(capabilityName)
                              .noFunction()
                              .build();
        Capability          capability = Capability.of(capabilityName, "", "");
        Map<String, Object> inputData;
        try {
            inputData = scheduledEventLog.getPayload() != null
                        ? objectMapper.convertValue(scheduledEventLog.getPayload(),
                                                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {})
                        : Map.of();
        } catch (Exception e) {
            inputData = Map.of();
        }
        LOG.infof("schedulePersistedEvent: re-dispatching case %s capability %s",
                  caseId, capabilityName);
        submit(eventLogId, instance, worker, capability, inputData, bindingName);
    }

}
