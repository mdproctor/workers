package io.casehub.workers.k8s;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.CaseInstanceRepository;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class K8sJobInformerManager {

    private static final Logger LOG = Logger.getLogger(K8sJobInformerManager.class);

    @Inject KubernetesClient kubernetesClient;
    @Inject AsyncWorkerCompletionRegistry registry;
    @Inject WorkflowCompletionPublisher completionPublisher;
    @Inject WorkerFaultPublisher faultPublisher;
    @Inject CaseInstanceRepository caseInstanceRepository;
    @Inject JobDefinitionResolver resolver;

    private final Map<String, SharedIndexInformer<Job>> informers = new ConcurrentHashMap<>();
    private final Set<String> failedNamespaces = ConcurrentHashMap.newKeySet();
    private final Set<String> recoveredDispatchIds = ConcurrentHashMap.newKeySet();

    void start(Set<String> namespaces) {
        for (String ns : namespaces) {
            try {
                SharedIndexInformer<Job> informer = kubernetesClient.resources(Job.class)
                    .inNamespace(ns)
                    .withLabel(K8sWorkerConstants.MANAGED_BY_LABEL, K8sWorkerConstants.MANAGED_BY_VALUE)
                    .inform(new ResourceEventHandler<>() {
                        @Override
                        public void onAdd(Job job) {
                            handleTerminalEvent(job);
                        }

                        @Override
                        public void onUpdate(Job oldJob, Job newJob) {
                            handleTerminalEvent(newJob);
                        }

                        @Override
                        public void onDelete(Job job, boolean deletedFinalStateUnknown) {
                            handleDelete(job);
                        }
                    });
                informers.put(ns, informer);
                LOG.infof("Started informer for namespace '%s'", ns);
            } catch (Exception e) {
                LOG.warnf("Failed to start informer for namespace '%s': %s", ns, e.getMessage());
                failedNamespaces.add(ns);
            }
        }
    }

    void stop() {
        informers.values().forEach(informer -> {
            try {
                informer.close();
            } catch (Exception e) {
                LOG.warnf("Failed to close informer: %s", e.getMessage());
            }
        });
        informers.clear();
    }

    boolean isNamespaceAvailable(String namespace) {
        return !failedNamespaces.contains(namespace);
    }

    boolean hasActiveInformers() {
        return !informers.isEmpty();
    }

    private void handleTerminalEvent(Job job) {
        if (!isTerminal(job)) return;
        String dispatchId = extractDispatchId(job);
        if (dispatchId == null) return;

        Uni.createFrom().item(job)
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .subscribe().with(
                j -> processTerminal(j, dispatchId),
                err -> LOG.errorf(err, "Failed processing terminal Job for dispatch %s", dispatchId));
    }

    void handleDelete(Job job) {
        String dispatchId = extractDispatchId(job);
        if (dispatchId == null) return;

        if (isTerminal(job)) {
            Uni.createFrom().item(job)
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .subscribe().with(
                    j -> processTerminal(j, dispatchId),
                    err -> LOG.errorf(err, "Failed processing deleted terminal Job for dispatch %s", dispatchId));
            return;
        }

        Optional<PendingCompletion> maybePending = registry.complete(dispatchId);
        if (maybePending.isEmpty()) return;
        faultPublisher.fault(maybePending.get(), new RuntimeException("Job deleted externally"));
    }

    void processTerminal(Job job, String dispatchId) {
        Optional<PendingCompletion> maybePending = registry.complete(dispatchId);

        if (maybePending.isEmpty()) {
            maybePending = recoverFromJob(job, dispatchId);
        }

        if (maybePending.isEmpty()) return;

        PendingCompletion pending = maybePending.get();
        String ns = job.getMetadata().getNamespace();
        String jobName = job.getMetadata().getName();

        if (isSucceeded(job)) {
            String maxOutputStr = pending.provisionerMeta().getOrDefault("maxOutputBytes", "1048576");
            long maxOutput = Long.parseLong(maxOutputStr);
            Map<String, Object> output = K8sJobOutputCapture.capture(kubernetesClient, ns, jobName, maxOutput);
            completionPublisher.complete(pending.correlationContext(), output);
        } else {
            Throwable fault = classifyJobFailure(job, ns, jobName);
            faultPublisher.fault(pending, fault);
        }

        String cleanupStr = pending.provisionerMeta().getOrDefault("cleanup", "DELETE");
        if (!"RETAIN".equals(cleanupStr)) {
            try {
                kubernetesClient.resource(job).delete();
            } catch (Exception e) {
                LOG.warnf("Failed to delete Job %s/%s: %s", ns, jobName, e.getMessage());
            }
        }
    }

    Optional<PendingCompletion> recoverFromJob(Job job, String dispatchId) {
        if (!recoveredDispatchIds.add(dispatchId)) {
            return Optional.empty();
        }

        Map<String, String> labels = job.getMetadata().getLabels();
        String caseIdStr = labels.get(K8sWorkerConstants.CASE_ID_LABEL);
        String workerName = labels.get(K8sWorkerConstants.WORKER_NAME_LABEL);
        String capabilityTag = labels.get(K8sWorkerConstants.CAPABILITY_LABEL);
        String tenancyId = labels.get(K8sWorkerConstants.TENANCY_ID_LABEL);
        String eventLogIdStr = labels.get(K8sWorkerConstants.EVENT_LOG_ID_LABEL);
        String idempotency = labels.get(K8sWorkerConstants.IDEMPOTENCY_LABEL);

        Map<String, String> annotations = job.getMetadata().getAnnotations();
        String bindingName = annotations != null
            ? annotations.get(K8sWorkerConstants.BINDING_NAME_ANNOTATION) : null;

        if (caseIdStr == null || workerName == null || idempotency == null) {
            LOG.warnf("Job %s/%s missing recovery labels — cannot recover (pre-upgrade Job?)",
                job.getMetadata().getNamespace(), job.getMetadata().getName());
            recoveredDispatchIds.remove(dispatchId);
            return Optional.empty();
        }

        CaseInstance caseInstance;
        try {
            caseInstance = caseInstanceRepository.findByUuid(
                UUID.fromString(caseIdStr), tenancyId).orElse(null);
        } catch (Exception e) {
            LOG.warnf("Recovery: failed to load CaseInstance %s: %s", caseIdStr, e.getMessage());
            recoveredDispatchIds.remove(dispatchId);
            return Optional.empty();
        }

        if (caseInstance == null) {
            LOG.warnf("Recovery: CaseInstance %s not found — case may have been closed", caseIdStr);
            recoveredDispatchIds.remove(dispatchId);
            return Optional.empty();
        }

        Worker worker = Worker.builder()
            .name(workerName)
            .capabilityName(capabilityTag)
            .noFunction()
            .build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            caseInstance, worker, idempotency, tenancyId, bindingName);

        Map<String, String> provisionerMeta;
        try {
            JobDefinition def = resolver.resolve(capabilityTag, tenancyId);
            provisionerMeta = Map.of(
                "cleanup", def.cleanup().name(),
                "maxOutputBytes", String.valueOf(def.maxOutputBytes()));
        } catch (Exception e) {
            provisionerMeta = Map.of("cleanup", "DELETE", "maxOutputBytes", "1048576");
        }

        Long eventLogId = eventLogIdStr != null ? Long.parseLong(eventLogIdStr) : null;
        Capability capability = Capability.of(capabilityTag, "", "");

        LOG.infof("Recovery: reconstructed PendingCompletion for dispatch %s (case %s, worker %s)",
            dispatchId, caseIdStr, workerName);

        return Optional.of(new PendingCompletion(
            dispatchId, K8sWorkerConstants.WORKER_TYPE,
            ctx, "", capability, eventLogId,
            Instant.now(), Instant.MAX, provisionerMeta));
    }

    Throwable classifyJobFailure(Job job, String namespace, String jobName) {
        String reason = extractFailureReason(job);
        int backoffLimit = job.getSpec().getBackoffLimit() != null ? job.getSpec().getBackoffLimit() : 0;

        if ("DeadlineExceeded".equals(reason)) {
            String enrichment = enrichDeadlineExceeded(namespace, jobName);
            return new PermanentFaultException(0,
                "DeadlineExceeded" + (enrichment != null ? " — " + enrichment :
                    " — Job ran past activeDeadlineSeconds"));
        }

        if ("BackoffLimitExceeded".equals(reason)) {
            if (backoffLimit > 0) {
                return new PermanentFaultException(0,
                    "BackoffLimitExceeded after " + backoffLimit + " K8s retries");
            }
            String podReason = extractPodFailureReason(namespace, jobName);
            if (podReason != null) {
                return classifyPodReason(podReason, namespace, jobName);
            }
        }

        return new RuntimeException("Job failed: " + (reason != null ? reason : "unknown"));
    }

    private Throwable classifyPodReason(String reason, String namespace, String jobName) {
        return switch (reason) {
            case "OOMKilled" -> new PermanentFaultException(0, "OOMKilled — increase memory limit");
            case "ImagePullBackOff", "ErrImagePull" ->
                new PermanentFaultException(0, reason + " — check image name and registry credentials");
            case "InvalidImageName" ->
                new PermanentFaultException(0, "InvalidImageName — malformed image reference");
            case "CreateContainerConfigError" ->
                new PermanentFaultException(0, "CreateContainerConfigError — check volume mounts and ConfigMaps");
            case "Evicted", "Preempting" ->
                new RuntimeException(reason + " — transient infrastructure condition");
            default -> new RuntimeException("Pod failed: " + reason);
        };
    }

    private Pod findLastPod(String namespace, String jobName) {
        try {
            PodList pods = kubernetesClient.pods().inNamespace(namespace)
                .withLabel("job-name", jobName).list();
            if (pods.getItems().isEmpty()) return null;
            return pods.getItems().stream()
                .max(Comparator.comparing(p -> p.getMetadata().getCreationTimestamp()))
                .orElse(null);
        } catch (Exception e) {
            LOG.debugf("Could not find Pod for %s/%s: %s", namespace, jobName, e.getMessage());
            return null;
        }
    }

    private String enrichDeadlineExceeded(String namespace, String jobName) {
        Pod lastPod = findLastPod(namespace, jobName);
        if (lastPod == null) return null;
        List<ContainerStatus> statuses = lastPod.getStatus().getContainerStatuses();
        if (statuses != null && !statuses.isEmpty()) {
            var waiting = statuses.get(0).getState().getWaiting();
            if (waiting != null && waiting.getReason() != null) {
                return "Pod never started: " + waiting.getReason()
                    + (waiting.getMessage() != null ? " (" + waiting.getMessage() + ")" : "");
            }
        }
        return null;
    }

    private String extractPodFailureReason(String namespace, String jobName) {
        Pod lastPod = findLastPod(namespace, jobName);
        if (lastPod == null) return null;
        List<ContainerStatus> statuses = lastPod.getStatus().getContainerStatuses();
        if (statuses != null && !statuses.isEmpty()) {
            var terminated = statuses.get(0).getState().getTerminated();
            if (terminated != null && terminated.getReason() != null) {
                return terminated.getReason();
            }
            var waiting = statuses.get(0).getState().getWaiting();
            if (waiting != null && waiting.getReason() != null) {
                return waiting.getReason();
            }
        }
        return null;
    }

    private static String extractFailureReason(Job job) {
        if (job.getStatus() == null || job.getStatus().getConditions() == null) return null;
        return job.getStatus().getConditions().stream()
            .filter(c -> "Failed".equals(c.getType()) && "True".equals(c.getStatus()))
            .map(JobCondition::getReason)
            .findFirst().orElse(null);
    }

    private static String extractDispatchId(Job job) {
        Map<String, String> labels = job.getMetadata().getLabels();
        if (labels == null) return null;
        String dispatchId = labels.get(K8sWorkerConstants.DISPATCH_ID_LABEL);
        if (dispatchId == null) {
            LOG.debugf("Job %s/%s has managed-by label but no dispatch-id — ignoring",
                job.getMetadata().getNamespace(), job.getMetadata().getName());
        }
        return dispatchId;
    }

    static boolean isTerminal(Job job) {
        if (job.getStatus() == null || job.getStatus().getConditions() == null) return false;
        return job.getStatus().getConditions().stream()
            .anyMatch(c -> ("Complete".equals(c.getType()) || "Failed".equals(c.getType()))
                && "True".equals(c.getStatus()));
    }

    static boolean isSucceeded(Job job) {
        if (job.getStatus() == null || job.getStatus().getConditions() == null) return false;
        return job.getStatus().getConditions().stream()
            .anyMatch(c -> "Complete".equals(c.getType()) && "True".equals(c.getStatus()));
    }
}
