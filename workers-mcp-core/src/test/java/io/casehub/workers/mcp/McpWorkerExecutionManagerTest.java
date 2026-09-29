package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.engine.common.internal.history.EventLog;
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
class McpWorkerExecutionManagerTest {

    private McpWorkerExecutionManager manager;
    private McpServerResolver serverResolver;
    private McpSessionProvider sessionProvider;
    private WorkerFaultPublisher faultPublisher;
    private WorkflowCompletionPublisher completionPublisher;
    private HttpClient httpClient;

    private static final String CAP_TAG = "mcp:slack:send-message";
    private static final ResolvedMcpServer TEST_SERVER = new ResolvedMcpServer(
        "slack", "https://slack.internal/mcp", 30,
        Map.of("Authorization", "Bearer test-token"),
        java.util.Set.of("send-message", "list-channels"));

    @BeforeEach
    void setUp() {
        serverResolver = mock(McpServerResolver.class);
        sessionProvider = mock(McpSessionProvider.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        httpClient = mock(HttpClient.class);

        manager = new McpWorkerExecutionManager(serverResolver, sessionProvider, faultPublisher, completionPublisher);
        manager.httpClient = httpClient;

        when(serverResolver.resolve(eq(CAP_TAG), anyString())).thenReturn(TEST_SERVER);
        when(sessionProvider.getSession("slack"))
            .thenReturn(new McpSession("session-123", "2025-06-18"));
    }

    @Test
    void successfulToolCall_json_completesWithContent() throws Exception {
        String jsonRpcResponse = """
            {"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"Message sent"}]}}""";
        stubJsonResponse(200, jsonRpcResponse, "application/json", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general", "text", "hello"));

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).containsKey("content");
        verify(faultPublisher, never()).fault(any(), any(), anyLong(), any());
    }

    @Test
    void successfulToolCall_structuredContent_preferred() throws Exception {
        String jsonRpcResponse = """
            {"jsonrpc":"2.0","id":2,"result":{
              "content":[{"type":"text","text":"ignored"}],
              "structuredContent":{"status":"ok","messageId":"m-123"}
            }}""";
        stubJsonResponse(200, jsonRpcResponse, "application/json", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue())
            .containsEntry("status", "ok")
            .containsEntry("messageId", "m-123")
            .doesNotContainKey("content");
    }

    @Test
    void isError_true_retryableFault() throws Exception {
        String jsonRpcResponse = """
            {"jsonrpc":"2.0","id":2,"result":{"isError":true,"content":[{"type":"text","text":"Tool failed"}]}}""";
        stubJsonResponse(200, jsonRpcResponse, "application/json", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isNotInstanceOf(PermanentFaultException.class).hasMessageContaining("isError");
    }

    @Test
    void jsonRpcError_invalidParams_permanentFault() throws Exception {
        String jsonRpcResponse = """
            {"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"Invalid params"}}""";
        stubJsonResponse(200, jsonRpcResponse, "application/json", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void http404_withSession_retryable() throws Exception {
        stubResponse(404, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        verify(sessionProvider).invalidate("slack");
        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isNotInstanceOf(PermanentFaultException.class);
    }

    @Test
    void http404_withoutSession_permanentFault() throws Exception {
        when(sessionProvider.getSession("slack"))
            .thenReturn(new McpSession(null, "2025-06-18"));
        stubResponse(404, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        verify(sessionProvider, never()).invalidate(anyString());
        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void http429_retryAfter() throws Exception {
        stubResponse(429, "60");

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(60000L);
    }

    @Test
    void http400_permanentFault() throws Exception {
        stubResponse(400, null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void sseResponse_extractsJsonRpcResult() throws Exception {
        String sseBody = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"SSE result\"}]}}\n\n";
        stubJsonResponse(200, sseBody, "text/event-stream", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).containsKey("content");
    }

    @Test
    void protocolHeaders_sentCorrectly() throws Exception {
        String jsonRpcResponse = """
            {"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"ok"}]}}""";
        stubJsonResponse(200, jsonRpcResponse, "application/json", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"));

        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(captor.capture(), any(HttpResponse.BodyHandler.class));
        HttpRequest captured = captor.getValue();
        assertThat(captured.headers().firstValue("MCP-Protocol-Version")).hasValue("2025-06-18");
        assertThat(captured.headers().firstValue("Mcp-Session-Id")).hasValue("session-123");
        assertThat(captured.headers().firstValue("Authorization")).hasValue("Bearer test-token");
    }

    @Test void supports_delegatesToResolver() {
        when(serverResolver.canResolve("mcp:server1:tool1", "t1")).thenReturn(true);
        assertThat(manager.supports("mcp:server1:tool1", "t1")).isTrue();
    }

    @Test void getActiveWorkCount_returnsZero() {
        assertThat(manager.getActiveWorkCount("any")).isEqualTo(0);
    }

    @Test void schedulePersistedEvent_returnsVoid() {
        manager.schedulePersistedEvent(new EventLog());
    }

    @Test void submit_6arg_passesBindingName() throws Exception {
        String jsonRpcResponse = """
            {"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"ok"}]}}""";
        stubJsonResponse(200, jsonRpcResponse, "application/json", null);

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", CAP_TAG);
        Capability cap = WorkerTestSupport.testCapability(CAP_TAG);

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general"), "binding-x");

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor = ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionPublisher).complete(ctxCaptor.capture(), any());
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    private void stubResponse(int status, String retryAfter) throws Exception {
        Map<String, List<String>> headerMap = new HashMap<>();
        if (retryAfter != null) headerMap.put("Retry-After", List.of(retryAfter));
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.body()).thenReturn("");
        when(response.headers()).thenReturn(HttpHeaders.of(headerMap, (a, b) -> true));
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
    }

    private void stubJsonResponse(int status, String body, String contentType, String retryAfter) throws Exception {
        Map<String, List<String>> headerMap = new HashMap<>();
        if (contentType != null) headerMap.put("Content-Type", List.of(contentType));
        if (retryAfter != null) headerMap.put("Retry-After", List.of(retryAfter));
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.body()).thenReturn(body);
        when(response.headers()).thenReturn(HttpHeaders.of(headerMap, (a, b) -> true));
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
    }
}
