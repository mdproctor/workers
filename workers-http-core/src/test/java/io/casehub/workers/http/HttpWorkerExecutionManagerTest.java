package io.casehub.workers.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.testing.WorkerTestSupport;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class HttpWorkerExecutionManagerTest {

    private HttpWorkerExecutionManager manager;
    private HttpEndpointResolver httpEndpointResolver;
    private WorkerFaultPublisher faultPublisher;
    private AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry;
    private WorkflowCompletionPublisher completionPublisher;
    private HttpClient httpClient;

    private static final ResolvedEndpoint SYNC_ENDPOINT = new ResolvedEndpoint(
        "https://api.example.com/process", "POST", ExchangeMode.SYNC, Map.of(), 30);
    private static final ResolvedEndpoint ASYNC_ENDPOINT = new ResolvedEndpoint(
        "https://api.example.com/process", "POST", ExchangeMode.ASYNC, Map.of(), 30);

    @BeforeEach
    void setUp() {
        httpEndpointResolver = mock(HttpEndpointResolver.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        asyncWorkerCompletionRegistry = mock(AsyncWorkerCompletionRegistry.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        httpClient = mock(HttpClient.class);

        manager = new HttpWorkerExecutionManager(
            httpEndpointResolver, faultPublisher, asyncWorkerCompletionRegistry,
            completionPublisher, new ObjectMapper(), 60);
        manager.httpClient = httpClient;
    }

    @Test
    void sync_2xx_completesWithResponseBody() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(200, "{\"result\":\"ok\"}", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of("key", "val"));

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).containsEntry("result", "ok");
        verify(faultPublisher, never()).fault(any(WorkerCorrelationContext.class), any(), anyLong(), any());
    }

    @Test
    void sync_2xx_emptyBody_completesWithEmptyMap() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(200, "", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).isEmpty();
    }

    @Test
    void sync_2xx_nonJsonBody_completesWithEmptyMap() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(200, "not json", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).isEmpty();
    }

    @Test
    void sync_400_throwsPermanentFault() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(400, "", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
        assertThat(((PermanentFaultException) causeCaptor.getValue()).statusCode()).isEqualTo(400);
    }

    @Test
    void sync_404_throwsPermanentFault() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(404, "", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
        assertThat(((PermanentFaultException) causeCaptor.getValue()).statusCode()).isEqualTo(404);
    }

    @Test
    void sync_429_withRetryAfterSeconds() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(429, "", "30");

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(30000);
    }

    @Test
    void sync_429_withoutRetryAfter() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(429, "", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(RuntimeException.class)
            .isNotInstanceOf(RetryAfterException.class);
    }

    @Test
    void sync_500_throwsTransientFault() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(500, "", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(RuntimeException.class)
            .isNotInstanceOf(PermanentFaultException.class)
            .isNotInstanceOf(RetryAfterException.class)
            .hasMessageContaining("500");
    }

    @Test
    void sync_casehubHeaders_set() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(200, "{}", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        HttpRequest captured = captureRequest();
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.CASE_ID))
            .hasValue(instance.getUuid().toString());
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.TENANCY_ID))
            .hasValue(instance.tenancyId);
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.TASK_TYPE))
            .hasValue("cap");
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.IDEMPOTENCY)).isPresent();
    }

    @Test
    void async_2xx_registersAndFiresForget() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(ASYNC_ENDPOINT);
        stubResponse(200, "{}", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = stubPendingCompletion(instance, worker, cap);
        when(asyncWorkerCompletionRegistry.register(
            eq(HttpWorkerConstants.WORKER_TYPE),
            any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), eq(Duration.ofMinutes(60)), eq(Map.of())))
            .thenReturn(pending);

        manager.submit(1L, instance, worker, cap, Map.of("key", "val"));

        verify(asyncWorkerCompletionRegistry).register(
            eq(HttpWorkerConstants.WORKER_TYPE),
            any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), any(Duration.class), eq(Map.of()));
        verify(completionPublisher, never()).complete(any(), any());
        verify(faultPublisher, never()).fault(any(WorkerCorrelationContext.class), any(), anyLong(), any());
    }

    @Test
    void async_nonOk_firesFault() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(ASYNC_ENDPOINT);
        stubResponse(500, "", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = stubPendingCompletion(instance, worker, cap);
        when(asyncWorkerCompletionRegistry.register(
            eq(HttpWorkerConstants.WORKER_TYPE),
            any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), eq(Duration.ofMinutes(60)), eq(Map.of())))
            .thenReturn(pending);

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("500");
    }

    @Test
    void async_headersIncludeWorkerIdAndCallbackToken() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(ASYNC_ENDPOINT);
        stubResponse(200, "{}", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = stubPendingCompletion(instance, worker, cap);
        when(asyncWorkerCompletionRegistry.register(
            eq(HttpWorkerConstants.WORKER_TYPE),
            any(WorkerCorrelationContext.class),
            eq(cap), eq(1L), eq(Duration.ofMinutes(60)), eq(Map.of())))
            .thenReturn(pending);

        manager.submit(1L, instance, worker, cap, Map.of());

        HttpRequest captured = captureRequest();
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.WORKER_ID))
            .hasValue(pending.dispatchId());
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.CALLBACK_TOKEN))
            .hasValue(pending.callbackToken());
    }

    @Test
    void sync_headersDoNotIncludeWorkerIdOrCallbackToken() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(200, "{}", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        HttpRequest captured = captureRequest();
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.WORKER_ID)).isEmpty();
        assertThat(captured.headers().firstValue(CasehubWorkerHeaders.CALLBACK_TOKEN)).isEmpty();
    }

    @Test
    void uriTemplate_interpolated() {
        String result = HttpWorkerExecutionManager.interpolateUrl(
            "https://api.example.com/orders/{orderId}/ship",
            Map.of("orderId", "123"));
        assertThat(result).isEqualTo("https://api.example.com/orders/123/ship");
    }

    @Test
    void uriTemplate_missingKey_throwsPermanentFault() {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(new ResolvedEndpoint(
            "https://api.example.com/{missing}", "POST", ExchangeMode.SYNC, Map.of(), 30));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isInstanceOf(PermanentFaultException.class)
            .hasMessageContaining("{missing}");
    }

    @Test
    void uriTemplate_noPlaceholders_passthrough() {
        String url = "https://example.com/api";
        String result = HttpWorkerExecutionManager.interpolateUrl(url, Map.of());
        assertThat(result).isEqualTo(url);
    }

    @Test
    void uriTemplate_urlEncoded() {
        String result = HttpWorkerExecutionManager.interpolateUrl(
            "https://api.example.com/search/{query}",
            Map.of("query", "hello world&foo=bar"));
        assertThat(result).isEqualTo("https://api.example.com/search/hello+world%26foo%3Dbar");
    }

    @Test
    void supports_delegatesToResolver() {
        when(httpEndpointResolver.canResolve("endpoint-1", "t1")).thenReturn(true);
        when(httpEndpointResolver.canResolve("endpoint-2", "t1")).thenReturn(false);

        assertThat(manager.supports("endpoint-1", "t1")).isTrue();
        assertThat(manager.supports("endpoint-2", "t1")).isFalse();
    }

    @Test
    void getActiveWorkCount_delegatesToRegistry() {
        when(asyncWorkerCompletionRegistry.countByWorkerName("w1")).thenReturn(3);
        assertThat(manager.getActiveWorkCount("w1")).isEqualTo(3);
    }

    @Test
    void sync_connectionRefused_firesFault() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new IOException("Connection refused"));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(IOException.class);
        verify(completionPublisher, never()).complete(any(), any());
    }

    @Test
    void async_429_retryAfterCappedToRemainingTtl() throws Exception {
        ResolvedEndpoint asyncEndpoint = new ResolvedEndpoint(
            "https://api.example.com/process", "POST", ExchangeMode.ASYNC, Map.of(), 30);
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(asyncEndpoint);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        PendingCompletion pending = new PendingCompletion(
            "dispatch-123", HttpWorkerConstants.WORKER_TYPE,
            new WorkerCorrelationContext(instance, worker, "idem", instance.tenancyId, null),
            "token", cap, 1L, Instant.now(),
            Instant.now().plusSeconds(60), Map.of());
        when(asyncWorkerCompletionRegistry.register(eq(HttpWorkerConstants.WORKER_TYPE), any(WorkerCorrelationContext.class), any(Capability.class), any(), any(), any()))
            .thenReturn(pending);

        stubResponse(429, "", "3600");

        manager.submit(1L, instance, worker, cap, Map.of());

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(WorkerCorrelationContext.class), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        RetryAfterException ra = (RetryAfterException) causeCaptor.getValue();
        assertThat(ra.retryAfterMs()).isLessThanOrEqualTo(60_000L);
    }

    @Test
    void submit_missingRoute_firesFault() {
        when(httpEndpointResolver.resolve(eq("missing"), anyString()))
            .thenThrow(WorkerProvisioningException.noRouteFound("missing"));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "missing");
        Capability cap = WorkerTestSupport.testCapability("missing");

        manager.submit(1L, instance, worker, cap, Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class), eq(cap), eq(1L),
            any(WorkerProvisioningException.class));
        verify(completionPublisher, never()).complete(any(), any());
    }

    @Test
    void submit_6arg_passesBindingNameThroughCompletion() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(200, "{\"result\":\"ok\"}", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of("key", "val"), "binding-x");

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionPublisher).complete(ctxCaptor.capture(), any());
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    @Test
    void submit_5arg_passesNullBindingName() throws Exception {
        when(httpEndpointResolver.resolve(eq("cap"), anyString())).thenReturn(SYNC_ENDPOINT);
        stubResponse(200, "{\"result\":\"ok\"}", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");

        manager.submit(1L, instance, worker, cap, Map.of("key", "val"));

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionPublisher).complete(ctxCaptor.capture(), any());
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    private PendingCompletion stubPendingCompletion(CaseInstance instance, Worker worker, Capability cap) {
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, "test-idempotency", instance.tenancyId, null);
        return new PendingCompletion(
            "dispatch-123", HttpWorkerConstants.WORKER_TYPE,
            ctx, "callback-token-abc", cap, 1L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
    }

    private void stubResponse(int status, String body, String retryAfter) throws Exception {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.body()).thenReturn(body);
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
