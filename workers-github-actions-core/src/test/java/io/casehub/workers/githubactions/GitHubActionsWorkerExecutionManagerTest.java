package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.testing.WorkerTestSupport;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class GitHubActionsWorkerExecutionManagerTest {

    private GitHubActionsWorkerExecutionManager manager;
    private GitHubActionsTokenResolver tokenResolver;
    private WorkerFaultPublisher faultPublisher;
    private WorkflowCompletionPublisher completionPublisher;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() throws Exception {
        tokenResolver = mock(GitHubActionsTokenResolver.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        httpClient = mock(HttpClient.class);

        manager = new GitHubActionsWorkerExecutionManager(tokenResolver, faultPublisher, completionPublisher);
        manager.httpClient = httpClient;

        when(tokenResolver.resolve(anyString())).thenReturn("ghp_test_token");
        when(tokenResolver.apiBaseUrl()).thenReturn("https://api.github.com");
    }

    @Test
    void workflowDispatch_204_completesWithOutput() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue())
            .containsEntry("dispatched", true)
            .containsEntry("owner", "casehubio")
            .containsEntry("repo", "devtown");
        verify(faultPublisher, never()).fault(any(), any(), anyLong(), any());
    }

    @Test
    void workflowDispatch_correctUrl() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        HttpRequest captured = captureRequest();
        assertThat(captured.uri().toString()).isEqualTo(
            "https://api.github.com/repos/casehubio/devtown/actions/workflows/ci.yml/dispatches");
        assertThat(captured.method()).isEqualTo("POST");
    }

    @Test
    void repositoryDispatch_204_completesWithOutput() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "event_type", "upstream-published"));

        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), any());
    }

    @Test
    void repositoryDispatch_correctUrl() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "event_type", "upstream-published"));

        HttpRequest captured = captureRequest();
        assertThat(captured.uri().toString()).isEqualTo(
            "https://api.github.com/repos/casehubio/devtown/dispatches");
    }

    @Test
    void workflowDispatch_missingOwner_permanentFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("repo", "devtown", "workflow_id", "ci.yml", "ref", "main"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void workflowDispatch_missingRef_permanentFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown", "workflow_id", "ci.yml"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void repositoryDispatch_missingEventType_permanentFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void workflowDispatch_422_retryAfter60s() throws Exception {
        stubResponse(422, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(60000L);
    }

    @Test
    void repositoryDispatch_422_permanentFault() throws Exception {
        stubResponse(422, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "event_type", "upstream-published"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void response_429_withRetryAfter() throws Exception {
        stubResponse(429, "30");

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(30000L);
    }

    @Test
    void response_403_permanentFault() throws Exception {
        stubResponse(403, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
        assertThat(((PermanentFaultException) causeCaptor.getValue()).statusCode()).isEqualTo(403);
    }

    @Test
    void response_500_retryableFault() throws Exception {
        stubResponse(500, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isNotInstanceOf(PermanentFaultException.class)
            .isNotInstanceOf(RetryAfterException.class);
    }

    @Test
    void customApiBaseUrl_usedInUrl() throws Exception {
        when(tokenResolver.apiBaseUrl()).thenReturn("https://github.example.com/api/v3");
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        HttpRequest captured = captureRequest();
        assertThat(captured.uri().toString()).isEqualTo(
            "https://github.example.com/api/v3/repos/casehubio/devtown/actions/workflows/ci.yml/dispatches");
    }

    @Test
    void headers_includeAuthAndGitHubApi() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        HttpRequest captured = captureRequest();
        assertThat(captured.headers().firstValue("Authorization")).hasValue("Bearer ghp_test_token");
        assertThat(captured.headers().firstValue("Accept")).hasValue("application/vnd.github+json");
        assertThat(captured.headers().firstValue("X-GitHub-Api-Version")).hasValue("2022-11-28");
        assertThat(captured.headers().firstValue("Content-Type")).hasValue("application/json");
    }

    @Test
    void submit_6arg_passesBindingNameThroughCompletion() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"), "binding-x");

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionPublisher).complete(ctxCaptor.capture(), any());
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    @Test
    void submit_5arg_passesNullBindingName() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionPublisher).complete(ctxCaptor.capture(), any());
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    @Test
    void supports_returnsTrueForBothCapabilities() {
        assertThat(manager.supports(GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH, "t1")).isTrue();
        assertThat(manager.supports(GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH, "t1")).isTrue();
        assertThat(manager.supports("unknown-capability", "t1")).isFalse();
    }

    @Test
    void getActiveWorkCount_returnsZero() {
        assertThat(manager.getActiveWorkCount("any")).isEqualTo(0);
    }

    @Test
    void request_hasTimeout() throws Exception {
        stubResponse(204, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"));

        HttpRequest captured = captureRequest();
        assertThat(captured.timeout()).isPresent();
    }

    private void stubResponse(int status, String retryAfter) throws Exception {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.body()).thenReturn("");
        Map<String, List<String>> headerMap = new HashMap<>();
        if (retryAfter != null) {
            headerMap.put("Retry-After", List.of(retryAfter));
        }
        when(response.headers()).thenReturn(HttpHeaders.of(headerMap, (a, b) -> true));
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(response);
    }

    private HttpRequest captureRequest() throws Exception {
        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(captor.capture(), any(HttpResponse.BodyHandler.class));
        return captor.getValue();
    }
}
