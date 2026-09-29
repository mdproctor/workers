package io.casehub.workers.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.CaseInstanceRepository;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.testing.WorkerTestSupport;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class K8sWorkerExecutionManagerTest {

    private static final UUID CASE_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    K8sWorkerExecutionManager manager;
    JobDefinitionResolver resolver;
    AsyncWorkerCompletionRegistry registry;
    WorkerFaultPublisher faultPublisher;
    KubernetesClient client;
    K8sJobInformerManager informerManager;
    CaseInstanceRepository caseInstanceRepository;
    MixedOperation jobOp;
    NonNamespaceOperation jobNsOp;
    FilterWatchListDeletable jobLabelOp;

    @BeforeEach
    void setUp() {
        manager = new K8sWorkerExecutionManager();
        resolver = new JobDefinitionResolver("default", 3600, 600, 0, "delete", 1_048_576, 262_144);
        registry = mock(AsyncWorkerCompletionRegistry.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        client = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
        informerManager = mock(K8sJobInformerManager.class);
        caseInstanceRepository = mock(CaseInstanceRepository.class);
        jobOp = mock(MixedOperation.class);
        jobNsOp = mock(NonNamespaceOperation.class);
        jobLabelOp = mock(FilterWatchListDeletable.class);

        when(client.resources(Job.class)).thenReturn(jobOp);
        when(jobOp.inNamespace(anyString())).thenReturn(jobNsOp);
        when(jobNsOp.withLabels(anyMap())).thenReturn(jobLabelOp);

        manager.resolver = resolver;
        manager.registry = registry;
        manager.faultPublisher = faultPublisher;
        manager.kubernetesClient = client;
        manager.informerManager = informerManager;
        manager.caseInstanceRepository = caseInstanceRepository;
        manager.objectMapper = new ObjectMapper();
        manager.maxInputBytes = 262144;

        when(informerManager.isNamespaceAvailable(anyString())).thenReturn(true);
    }

    private static JobDefinition imageDef(String name) {
        return new JobDefinition(name, "batch", "acme/" + name + ":latest",
            List.of(), List.of(), null, null, null, null, null,
            3600, 600, 0, 1_048_576, null, Map.of(), Map.of(), CleanupPolicy.DELETE);
    }

    @Test
    void submit_createsJobAndRegisters() {
        resolver.initialize(Map.of("report-gen", imageDef("report-gen")));
        PendingCompletion pending = mock(PendingCompletion.class);
        when(pending.dispatchId()).thenReturn("dispatch-1");
        when(registry.register(anyString(), any(), any(), any(), any(), any()))
            .thenReturn(pending);
        NamespaceableResource<Job> resource = mock(NamespaceableResource.class);
        when(client.resource(any(Job.class))).thenReturn(resource);
        when(resource.create()).thenReturn(new Job());

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "k8s:report-gen"),
            WorkerTestSupport.testCapability("k8s:report-gen"),
            Map.of("key", "value"));

        verify(registry).register(eq(K8sWorkerConstants.WORKER_TYPE),
            any(), any(), eq(1L), any(Duration.class), any());
        verify(resource).create();
    }

    @Test
    void submit_unknownCapability_publishesPermanentFault() {
        resolver.initialize(Map.of());

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "k8s:missing"),
            WorkerTestSupport.testCapability("k8s:missing"),
            Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class), any(Capability.class), eq(1L),
            any(PermanentFaultException.class));
    }

    @Test
    void submit_createFailure403_cleansUpRegistryAndFaultsPermanent() {
        resolver.initialize(Map.of("test", imageDef("test")));
        PendingCompletion pending = mock(PendingCompletion.class);
        when(pending.dispatchId()).thenReturn("dispatch-1");
        when(registry.register(anyString(), any(), any(), any(), any(), any()))
            .thenReturn(pending);
        NamespaceableResource<Job> resource = mock(NamespaceableResource.class);
        when(client.resource(any(Job.class))).thenReturn(resource);
        when(resource.create()).thenThrow(new KubernetesClientException("Forbidden", 403, null));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "k8s:test"),
            WorkerTestSupport.testCapability("k8s:test"),
            Map.of());

        verify(registry).complete("dispatch-1");
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(
            any(), any(), eq(1L), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void submit_createFailure409_cleansUpRegistryAndFaultsRetryable() {
        resolver.initialize(Map.of("test", imageDef("test")));
        PendingCompletion pending = mock(PendingCompletion.class);
        when(pending.dispatchId()).thenReturn("dispatch-1");
        when(registry.register(anyString(), any(), any(), any(), any(), any()))
            .thenReturn(pending);
        NamespaceableResource<Job> resource = mock(NamespaceableResource.class);
        when(client.resource(any(Job.class))).thenReturn(resource);
        when(resource.create()).thenThrow(new KubernetesClientException("Conflict", 409, null));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "k8s:test"),
            WorkerTestSupport.testCapability("k8s:test"),
            Map.of());

        verify(registry).complete("dispatch-1");
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(
            any(), any(), eq(1L), captor.capture());
        assertThat(captor.getValue()).isNotInstanceOf(PermanentFaultException.class);
    }

    @Test
    void submit_inputDataExceedsMaxBytes_permanentFault() {
        resolver.initialize(Map.of("test", imageDef("test")));
        manager.maxInputBytes = 10;

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "k8s:test"),
            WorkerTestSupport.testCapability("k8s:test"),
            Map.of("large", "x".repeat(100)));

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(
            any(), any(), eq(1L), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(PermanentFaultException.class);
        assertThat(captor.getValue().getMessage()).contains("exceeds maxInputBytes");
    }

    // --- bindingName propagation tests ---

    @Test
    void submit_6arg_passesBindingNameThroughFault() {
        resolver.initialize(Map.of());

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "k8s:missing"),
            WorkerTestSupport.testCapability("k8s:missing"),
            Map.of(), "binding-x");

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(faultPublisher).fault(
            ctxCaptor.capture(), any(Capability.class), eq(1L),
            any(PermanentFaultException.class));
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    @Test
    void submit_5arg_passesNullBindingName() {
        resolver.initialize(Map.of());

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "k8s:missing"),
            WorkerTestSupport.testCapability("k8s:missing"),
            Map.of());

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(faultPublisher).fault(
            ctxCaptor.capture(), any(Capability.class), eq(1L),
            any(PermanentFaultException.class));
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    @Test
    void supports_delegatesToResolver() {
        resolver.initialize(Map.of("x", imageDef("x")));

        assertThat(manager.supports("k8s:x", "t1")).isTrue();
        assertThat(manager.supports("k8s:y", "t1")).isFalse();
    }

    @Test
    void getActiveWorkCount_delegatesToRegistry() {
        when(registry.countByWorkerName("w1")).thenReturn(3);

        assertThat(manager.getActiveWorkCount("w1")).isEqualTo(3);
    }

    @Test
    void schedulePersistedEvent_noExistingJob_reDispatches() {
        resolver.initialize(Map.of("test", imageDef("test")));
        EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:test", 1L);
        CaseInstance instance = WorkerTestSupport.testCaseInstance("t1");
        instance.setUuid(CASE_ID);

        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(instance));
        mockK8sJobListEmpty();
        mockJobCreation();

        manager.schedulePersistedEvent(eventLog);

        verify(registry).register(eq(K8sWorkerConstants.WORKER_TYPE),
            any(), any(), eq(1L), any(Duration.class), any());
    }

    @Test
    void schedulePersistedEvent_existingRunningJob_skips() {
        resolver.initialize(Map.of("test", imageDef("test")));
        EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:test", 1L);
        mockK8sJobListReturns(buildRunningK8sJob());

        manager.schedulePersistedEvent(eventLog);

        verify(registry, never()).register(any(), any(), any(), any(), any(), any());
    }

    @Test
    void schedulePersistedEvent_caseNotFound_skips() {
        resolver.initialize(Map.of("test", imageDef("test")));
        EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:test", 1L);
        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.empty());
        mockK8sJobListEmpty();

        manager.schedulePersistedEvent(eventLog);

        verify(registry, never()).register(any(), any(), any(), any(), any(), any());
    }

    @Test
    void schedulePersistedEvent_capabilityNotResolvable_skips() {
        resolver.initialize(Map.of());
        EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:missing", 1L);

        manager.schedulePersistedEvent(eventLog);

        verify(registry, never()).register(any(), any(), any(), any(), any(), any());
    }

    // --- bindingName in schedulePersistedEvent ---

    @Test
    void schedulePersistedEvent_readsBindingNameFromMetadata() {
        resolver.initialize(Map.of("test", imageDef("test")));
        EventLog eventLog = buildScheduledEventLogWithBindingName(CASE_ID, "t1", "w1",
            "k8s:test", 1L, "spe-binding");
        CaseInstance instance = WorkerTestSupport.testCaseInstance("t1");
        instance.setUuid(CASE_ID);

        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(instance));
        mockK8sJobListEmpty();
        mockJobCreation();

        manager.schedulePersistedEvent(eventLog);

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(registry).register(eq(K8sWorkerConstants.WORKER_TYPE),
            ctxCaptor.capture(), any(), eq(1L), any(Duration.class), any());
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("spe-binding");
    }

    @Test
    void schedulePersistedEvent_noBindingNameInMetadata_passesNull() {
        resolver.initialize(Map.of("test", imageDef("test")));
        EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:test", 1L);
        CaseInstance instance = WorkerTestSupport.testCaseInstance("t1");
        instance.setUuid(CASE_ID);

        when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
            .thenReturn(java.util.Optional.of(instance));
        mockK8sJobListEmpty();
        mockJobCreation();

        manager.schedulePersistedEvent(eventLog);

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(registry).register(eq(K8sWorkerConstants.WORKER_TYPE),
            ctxCaptor.capture(), any(), eq(1L), any(Duration.class), any());
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    private EventLog buildScheduledEventLogWithBindingName(UUID caseId, String tenancyId,
            String workerName, String capabilityName, Long eventLogId, String bindingName) {
        EventLog eventLog = new EventLog();
        eventLog.setCaseId(caseId);
        eventLog.tenancyId = tenancyId;
        eventLog.id = eventLogId;
        ObjectMapper mapper = new ObjectMapper();
        try {
            eventLog.setMetadata(mapper.readTree(
                "{\"workerName\":\"" + workerName + "\",\"capabilityName\":\"" + capabilityName
                    + "\",\"bindingName\":\"" + bindingName + "\"}"));
            eventLog.setPayload(mapper.readTree("{\"key\":\"value\"}"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return eventLog;
    }

    private EventLog buildScheduledEventLog(UUID caseId, String tenancyId,
            String workerName, String capabilityName, Long eventLogId) {
        EventLog eventLog = new EventLog();
        eventLog.setCaseId(caseId);
        eventLog.tenancyId = tenancyId;
        eventLog.id = eventLogId;
        ObjectMapper mapper = new ObjectMapper();
        try {
            eventLog.setMetadata(mapper.readTree(
                "{\"workerName\":\"" + workerName + "\",\"capabilityName\":\"" + capabilityName + "\"}"));
            eventLog.setPayload(mapper.readTree("{\"key\":\"value\"}"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return eventLog;
    }

    private void mockK8sJobListEmpty() {
        JobList jobList = new JobList();
        jobList.setItems(List.of());
        when(jobLabelOp.list()).thenReturn(jobList);
    }

    private void mockK8sJobListReturns(Job job) {
        JobList jobList = new JobList();
        jobList.setItems(List.of(job));
        when(jobLabelOp.list()).thenReturn(jobList);
    }

    private Job buildRunningK8sJob() {
        Job job = new Job();
        job.setMetadata(new ObjectMeta());
        job.getMetadata().setName("casehub-test-running");
        job.setStatus(new JobStatus());
        return job;
    }

    private void mockJobCreation() {
        PendingCompletion pending = mock(PendingCompletion.class);
        when(pending.dispatchId()).thenReturn("dispatch-recovery");
        when(registry.register(anyString(), any(), any(), any(), any(), any()))
            .thenReturn(pending);
        NamespaceableResource<Job> resource = mock(NamespaceableResource.class);
        when(client.resource(any(Job.class))).thenReturn(resource);
        when(resource.create()).thenReturn(new Job());
    }
}
