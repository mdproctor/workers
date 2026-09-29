package io.casehub.workers.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.CaseInstanceRepository;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.smallrye.mutiny.Uni;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class K8sJobInformerManagerTest {

    private static final UUID CASE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    K8sJobInformerManager manager;
    AsyncWorkerCompletionRegistry registry;
    WorkflowCompletionPublisher completionPublisher;
    WorkerFaultPublisher faultPublisher;
    KubernetesClient client;
    MixedOperation podOp;
    NonNamespaceOperation nsOp;
    FilterWatchListDeletable labelOp;
    CaseInstance testCaseInstance;
    CaseInstanceRepository caseInstanceRepository;
    JobDefinitionResolver resolver;

    @BeforeEach
    void setUp() {
        manager = new K8sJobInformerManager();
        registry = mock(AsyncWorkerCompletionRegistry.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        client = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
        podOp = mock(MixedOperation.class);
        nsOp = mock(NonNamespaceOperation.class);
        labelOp = mock(FilterWatchListDeletable.class);

        when(client.pods()).thenReturn(podOp);
        when(podOp.inNamespace(anyString())).thenReturn(nsOp);
        when(nsOp.withLabel(anyString(), anyString())).thenReturn(labelOp);

        caseInstanceRepository = mock(CaseInstanceRepository.class);
        resolver = new JobDefinitionResolver("default", 3600, 600, 0, "delete", 1_048_576, 262_144);
        resolver.initialize(Map.of("test", imageDef("test")));
        testCaseInstance = new CaseInstance();
        testCaseInstance.setUuid(CASE_ID);
        testCaseInstance.tenancyId = "t1";

        manager.registry = registry;
        manager.completionPublisher = completionPublisher;
        manager.faultPublisher = faultPublisher;
        manager.kubernetesClient = client;
        manager.caseInstanceRepository = caseInstanceRepository;
        manager.resolver = resolver;
    }

    @Test
    void processTerminal_succeededJob_publishesCompletion() {
        Job job = buildJob("batch", "casehub-test-abc", "dispatch-1", "Complete");
        PendingCompletion pending = buildPending("dispatch-1", "DELETE");
        when(registry.complete("dispatch-1")).thenReturn(Optional.of(pending));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.processTerminal(job, "dispatch-1");

        verify(completionPublisher).complete(eq(pending.correlationContext()), any());
    }

    @Test
    void processTerminal_failedJob_publishesFault() {
        Job job = buildJob("batch", "casehub-test-abc", "dispatch-1", "Failed");
        job.getStatus().getConditions().get(0).setReason("DeadlineExceeded");
        PendingCompletion pending = buildPending("dispatch-1", "DELETE");
        when(registry.complete("dispatch-1")).thenReturn(Optional.of(pending));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.processTerminal(job, "dispatch-1");

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(eq(pending), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(PermanentFaultException.class);
        assertThat(captor.getValue().getMessage()).contains("DeadlineExceeded");
    }

    @Test
    void processTerminal_alreadyProcessed_doesNothing() {
        Job job = buildJob("batch", "casehub-test-abc", "dispatch-1", "Complete");
        when(registry.complete("dispatch-1")).thenReturn(Optional.empty());

        manager.processTerminal(job, "dispatch-1");

        verifyNoInteractions(completionPublisher, faultPublisher);
    }

    @Test
    void processTerminal_cleanupDelete_deletesJob() {
        Job job = buildJob("batch", "casehub-test-abc", "dispatch-1", "Complete");
        PendingCompletion pending = buildPending("dispatch-1", "DELETE");
        when(registry.complete("dispatch-1")).thenReturn(Optional.of(pending));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.processTerminal(job, "dispatch-1");

        verify(client.resource(job)).delete();
    }

    @Test
    void processTerminal_cleanupRetain_doesNotDelete() {
        Job job = buildJob("batch", "casehub-test-abc", "dispatch-1", "Complete");
        PendingCompletion pending = buildPending("dispatch-1", "RETAIN");
        when(registry.complete("dispatch-1")).thenReturn(Optional.of(pending));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.processTerminal(job, "dispatch-1");

        verify(client, never()).resource(any(Job.class));
    }

    @Test
    void classifyJobFailure_backoffLimitExceeded_backoffZero_retryable() {
        Job job = buildFailedJob("BackoffLimitExceeded", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isNotInstanceOf(PermanentFaultException.class);
    }

    @Test
    void classifyJobFailure_backoffLimitExceeded_backoffPositive_permanent() {
        Job job = buildFailedJob("BackoffLimitExceeded", 3);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void classifyJobFailure_deadlineExceeded_permanent() {
        Job job = buildFailedJob("DeadlineExceeded", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("DeadlineExceeded");
    }

    @Test
    void isTerminal_completeJob_true() {
        Job job = buildJob("ns", "j", "d", "Complete");
        assertThat(K8sJobInformerManager.isTerminal(job)).isTrue();
    }

    @Test
    void isTerminal_failedJob_true() {
        Job job = buildJob("ns", "j", "d", "Failed");
        assertThat(K8sJobInformerManager.isTerminal(job)).isTrue();
    }

    @Test
    void isTerminal_runningJob_false() {
        Job job = new Job();
        job.setMetadata(new ObjectMeta());
        job.setStatus(new JobStatus());
        assertThat(K8sJobInformerManager.isTerminal(job)).isFalse();
    }

    @Test
    void isTerminal_nullStatus_false() {
        Job job = new Job();
        job.setMetadata(new ObjectMeta());
        assertThat(K8sJobInformerManager.isTerminal(job)).isFalse();
    }

    @Test
    void isSucceeded_completeJob_true() {
        Job job = buildJob("ns", "j", "d", "Complete");
        assertThat(K8sJobInformerManager.isSucceeded(job)).isTrue();
    }

    @Test
    void isSucceeded_failedJob_false() {
        Job job = buildJob("ns", "j", "d", "Failed");
        assertThat(K8sJobInformerManager.isSucceeded(job)).isFalse();
    }

    @Test
    void classifyPodReason_OOMKilled_permanent() {
        Job job = buildFailedJobWithPodReason("OOMKilled", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("OOMKilled");
    }

    @Test
    void classifyPodReason_ImagePullBackOff_permanent() {
        Job job = buildFailedJobWithPodReason("ImagePullBackOff", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("ImagePullBackOff");
    }

    @Test
    void classifyPodReason_ErrImagePull_permanent() {
        Job job = buildFailedJobWithPodReason("ErrImagePull", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("ErrImagePull");
    }

    @Test
    void classifyPodReason_InvalidImageName_permanent() {
        Job job = buildFailedJobWithPodReason("InvalidImageName", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("InvalidImageName");
    }

    @Test
    void classifyPodReason_CreateContainerConfigError_permanent() {
        Job job = buildFailedJobWithPodReason("CreateContainerConfigError", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("CreateContainerConfigError");
    }

    @Test
    void classifyPodReason_Evicted_retryable() {
        Job job = buildFailedJobWithPodReason("Evicted", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isNotInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("Evicted");
    }

    @Test
    void classifyPodReason_Preempting_retryable() {
        Job job = buildFailedJobWithPodReason("Preempting", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isNotInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("Preempting");
    }

    @Test
    void classifyPodReason_unknown_retryable() {
        Job job = buildFailedJobWithPodReason("UnknownReason", 0);

        Throwable fault = manager.classifyJobFailure(job, "batch", "job-1");

        assertThat(fault).isNotInstanceOf(PermanentFaultException.class);
        assertThat(fault.getMessage()).contains("UnknownReason");
    }

    @Test
    void handleDelete_terminalJob_routesToProcessTerminal() {
        Job job = buildJob("batch", "casehub-test-abc", "dispatch-1", "Complete");
        PendingCompletion pending = buildPending("dispatch-1", "DELETE");
        when(registry.complete("dispatch-1")).thenReturn(Optional.of(pending));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.handleDelete(job);

        verify(completionPublisher, timeout(1000)).complete(eq(pending.correlationContext()), any());
    }

    @Test
    void handleDelete_nonTerminalJobWithPending_faultsJobDeletedExternally() {
        Job job = new Job();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("job-1");
        meta.setNamespace("batch");
        meta.setLabels(Map.of(K8sWorkerConstants.DISPATCH_ID_LABEL, "dispatch-1"));
        job.setMetadata(meta);
        job.setStatus(new JobStatus());

        PendingCompletion pending = buildPending("dispatch-1", "DELETE");
        when(registry.complete("dispatch-1")).thenReturn(Optional.of(pending));

        manager.handleDelete(job);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(eq(pending), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(RuntimeException.class);
        assertThat(captor.getValue().getMessage()).contains("Job deleted externally");
    }

    @Test
    void handleDelete_nonTerminalJobAlreadyProcessed_doesNothing() {
        Job job = new Job();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("job-1");
        meta.setNamespace("batch");
        meta.setLabels(Map.of(K8sWorkerConstants.DISPATCH_ID_LABEL, "dispatch-1"));
        job.setMetadata(meta);
        job.setStatus(new JobStatus());

        when(registry.complete("dispatch-1")).thenReturn(Optional.empty());

        manager.handleDelete(job);

        verifyNoInteractions(faultPublisher);
    }

    // --- helpers ---

    private Job buildJob(String namespace, String name, String dispatchId, String conditionType) {
        Job job = new Job();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        meta.setNamespace(namespace);
        meta.setLabels(Map.of(
            K8sWorkerConstants.DISPATCH_ID_LABEL, dispatchId,
            K8sWorkerConstants.MANAGED_BY_LABEL, K8sWorkerConstants.MANAGED_BY_VALUE));
        job.setMetadata(meta);

        JobSpec spec = new JobSpec();
        spec.setBackoffLimit(0);
        job.setSpec(spec);

        JobStatus status = new JobStatus();
        JobCondition condition = new JobCondition();
        condition.setType(conditionType);
        condition.setStatus("True");
        status.setConditions(List.of(condition));
        job.setStatus(status);
        return job;
    }

    private Job buildFailedJob(String reason, int backoffLimit) {
        Job job = buildJob("batch", "job-1", "d-1", "Failed");
        job.getStatus().getConditions().get(0).setReason(reason);
        job.getSpec().setBackoffLimit(backoffLimit);
        return job;
    }

    private Job buildFailedJobWithPodReason(String podReason, int backoffLimit) {
        Job job = buildFailedJob("BackoffLimitExceeded", backoffLimit);

        Pod pod = new Pod();
        ObjectMeta podMeta = new ObjectMeta();
        podMeta.setName("pod-1");
        podMeta.setCreationTimestamp("2026-07-02T10:00:00Z");
        pod.setMetadata(podMeta);

        PodStatus podStatus = new PodStatus();
        ContainerStatus containerStatus = new ContainerStatus();
        ContainerState state = new ContainerState();
        ContainerStateTerminated terminated = new ContainerStateTerminated();
        terminated.setReason(podReason);
        state.setTerminated(terminated);
        containerStatus.setState(state);
        podStatus.setContainerStatuses(List.of(containerStatus));
        pod.setStatus(podStatus);

        PodList podList = new PodList();
        podList.setItems(List.of(pod));
        when(labelOp.list()).thenReturn(podList);

        return job;
    }

    private PendingCompletion buildPending(String dispatchId, String cleanupPolicy) {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Capability capability = Capability.of("k8s:test", "", "");
        Worker worker = Worker.builder().name("w1")
            .capabilityName("k8s:test")
            .function(new WorkerFunction.Sync<>(Map.class, Map.class, (ctx, scope) -> WorkerResult.of(Map.of()))).build();
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash", "t1", null);
        return new PendingCompletion(dispatchId, K8sWorkerConstants.WORKER_TYPE,
            ctx, "token-1",
            capability, 1L,
            Instant.now(), Instant.now().plusSeconds(3600),
            Map.of("cleanup", cleanupPolicy));
    }

    private io.fabric8.kubernetes.api.model.PodList emptyPodList() {
        io.fabric8.kubernetes.api.model.PodList list = new io.fabric8.kubernetes.api.model.PodList();
        list.setItems(List.of());
        return list;
    }

    private Job buildRecoverableJob(String namespace, String name, String dispatchId,
            String conditionType, UUID caseId, String tenancyId,
            String workerName, String capability, Long eventLogId, String idempotency) {
        Job job = buildJob(namespace, name, dispatchId, conditionType);
        Map<String, String> labels = new LinkedHashMap<>(job.getMetadata().getLabels());
        labels.put(K8sWorkerConstants.CASE_ID_LABEL, caseId.toString());
        labels.put(K8sWorkerConstants.WORKER_NAME_LABEL, workerName);
        labels.put(K8sWorkerConstants.CAPABILITY_LABEL, capability);
        labels.put(K8sWorkerConstants.TENANCY_ID_LABEL, tenancyId);
        labels.put(K8sWorkerConstants.EVENT_LOG_ID_LABEL, String.valueOf(eventLogId));
        labels.put(K8sWorkerConstants.IDEMPOTENCY_LABEL, idempotency);
        job.getMetadata().setLabels(labels);
        return job;
    }

    private Job buildRecoverableJobWithAnnotation(String namespace, String name,
            String dispatchId, String conditionType, UUID caseId, String tenancyId,
            String workerName, String capability, Long eventLogId, String idempotency,
            String bindingName) {
        Job job = buildRecoverableJob(namespace, name, dispatchId, conditionType,
            caseId, tenancyId, workerName, capability, eventLogId, idempotency);
        if (bindingName != null) {
            Map<String, String> annotations = new LinkedHashMap<>();
            annotations.put(K8sWorkerConstants.BINDING_NAME_ANNOTATION, bindingName);
            job.getMetadata().setAnnotations(annotations);
        }
        return job;
    }

    private static JobDefinition imageDef(String name) {
        return new JobDefinition(name, "batch", "acme/" + name + ":latest",
            List.of(), List.of(), null, null, null, null, null,
            3600, 600, 0, 1_048_576, null, Map.of(), Map.of(), CleanupPolicy.DELETE);
    }

    // --- Recovery tests ---

    @Test
    void processTerminal_registryEmpty_recoversFromJobLabels() {
        Job job = buildRecoverableJob("batch", "casehub-test-abc", "dispatch-1",
            "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
        when(registry.complete("dispatch-1")).thenReturn(Optional.empty());
        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(testCaseInstance));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.processTerminal(job, "dispatch-1");

        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), any());
    }

    @Test
    void processTerminal_registryEmpty_missingCaseIdLabel_skips() {
        Job job = buildJob("batch", "casehub-test-abc", "dispatch-2", "Complete");
        when(registry.complete("dispatch-2")).thenReturn(Optional.empty());

        manager.processTerminal(job, "dispatch-2");

        verifyNoInteractions(completionPublisher, faultPublisher);
    }

    @Test
    void processTerminal_registryEmpty_secondCallSameDispatch_skips() {
        Job job = buildRecoverableJob("batch", "casehub-test-abc", "dispatch-3",
            "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
        when(registry.complete("dispatch-3")).thenReturn(Optional.empty());
        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(testCaseInstance));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.processTerminal(job, "dispatch-3");
        manager.processTerminal(job, "dispatch-3");

        verify(completionPublisher, times(1)).complete(any(), any());
    }

    @Test
    void processTerminal_registryEmpty_failedJob_publishesFault() {
        Job job = buildRecoverableJob("batch", "casehub-test-fail", "dispatch-4",
            "Failed", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
        job.getStatus().getConditions().get(0).setReason("DeadlineExceeded");
        when(registry.complete("dispatch-4")).thenReturn(Optional.empty());
        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(testCaseInstance));
        when(labelOp.list()).thenReturn(emptyPodList());

        manager.processTerminal(job, "dispatch-4");

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(PendingCompletion.class), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void processTerminal_registryEmpty_caseInstanceNotFound_skips() {
        Job job = buildRecoverableJob("batch", "casehub-test-abc", "dispatch-5",
            "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
        when(registry.complete("dispatch-5")).thenReturn(Optional.empty());
        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.empty());

        manager.processTerminal(job, "dispatch-5");

        verifyNoInteractions(completionPublisher, faultPublisher);
    }

    // --- bindingName annotation recovery tests ---

    @Test
    void recoverFromJob_readsBindingNameAnnotation() {
        Job job = buildRecoverableJobWithAnnotation("batch", "casehub-test-abc",
            "dispatch-6", "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L,
            "idem-hash", "recovered-binding");
        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(testCaseInstance));

        Optional<PendingCompletion> result = manager.recoverFromJob(job, "dispatch-6");

        assertThat(result).isPresent();
        assertThat(result.get().correlationContext().bindingName()).isEqualTo("recovered-binding");
    }

    @Test
    void recoverFromJob_preUpgradeJob_nullBindingName() {
        Job job = buildRecoverableJob("batch", "casehub-test-abc", "dispatch-7",
            "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
        // No annotation — pre-upgrade Job
        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(testCaseInstance));

        Optional<PendingCompletion> result = manager.recoverFromJob(job, "dispatch-7");

        assertThat(result).isPresent();
        assertThat(result.get().correlationContext().bindingName()).isNull();
    }
}
