package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class McpWorkerRuntimeTest {

    private McpWorkerRuntime runtime;
    private McpServerResolver serverResolver;
    private McpSessionManager sessionManager;
    private WebClient webClient;
    private HttpRequest<Buffer> request;

    @BeforeEach
    void setUp() {
        serverResolver = new McpServerResolver();
        sessionManager = mock(McpSessionManager.class);
        webClient = mock(WebClient.class);
        request = mock(HttpRequest.class);

        runtime = new McpWorkerRuntime();
        runtime.serverResolver = serverResolver;
        runtime.sessionManager = sessionManager;
        runtime.webClient = webClient;

        when(webClient.requestAbs(any(HttpMethod.class), anyString())).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
        when(request.timeout(anyLong())).thenReturn(request);
    }

    @Test
    void initialStatus_isPending() {
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("mcp");
    }

    @Test
    void initialize_discoveryAuto_callsToolsList() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")
        ), 30);

        McpSession session = new McpSession("session-abc", McpWorkerConstants.PROTOCOL_VERSION);
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        HttpResponse<Buffer> toolsResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":["
                + "{\"name\":\"send-message\",\"description\":\"Send a message\"},"
                + "{\"name\":\"list-channels\",\"description\":\"List channels\"}"
                + "]}}");
        when(toolsResponse.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(toolsResponse));

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities())
            .containsExactlyInAnyOrder("mcp:slack:send-message", "mcp:slack:list-channels");
    }

    @Test
    void initialize_discoveryManual_skipsToolsList() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "manual")
        ), 30);

        McpSession session = new McpSession("session-abc", McpWorkerConstants.PROTOCOL_VERSION);
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactlyInAnyOrder("mcp:slack:send-message");
        // No WebClient call should have been made
        verify(webClient, never()).requestAbs(any(HttpMethod.class), anyString());
    }

    @Test
    void initialize_sessionFails_faulted() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")
        ), 30);

        when(sessionManager.getOrInitialize("slack"))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("Connection refused")));

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
        assertThat(runtime.capabilities()).isEmpty();
    }

    @Test
    void initialize_partialFailure_running() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto"),
            new McpServerResolver.ServerConfig("jira", "https://jira.internal/mcp", "", 30, Map.of(), "auto")
        ), 30);

        McpSession slackSession = new McpSession("session-slack", McpWorkerConstants.PROTOCOL_VERSION);
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(slackSession));
        when(sessionManager.getOrInitialize("jira"))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("Connection refused")));

        HttpResponse<Buffer> toolsResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":["
                + "{\"name\":\"send-message\",\"description\":\"Send a message\"}"
                + "]}}");
        when(toolsResponse.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(toolsResponse));

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("mcp:slack:send-message");
    }

    @Test
    void initialize_toolsListError_fallsBackToConfig() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")
        ), 30);

        McpSession session = new McpSession("session-abc", McpWorkerConstants.PROTOCOL_VERSION);
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        // tools/list returns a JSON-RPC error
        HttpResponse<Buffer> errorResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{\"code\":-32601,\"message\":\"Method not found\"}}");
        when(errorResponse.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(errorResponse));

        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        // Falls back to config-declared tools
        assertThat(runtime.capabilities()).containsExactly("mcp:slack:send-message");
    }

    @Test
    void initialize_alreadyRunning_isNoOp() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "manual")
        ), 30);

        McpSession session = new McpSession("session-abc", McpWorkerConstants.PROTOCOL_VERSION);
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);

        // Second call — verify no additional session initialization
        runtime.initialize();
        verify(sessionManager, org.mockito.Mockito.times(1)).getOrInitialize("slack");
    }

    @Test
    void initialize_faultedThenServerRecovers_transitionsToRunning() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "manual")
        ), 30);

        when(sessionManager.getOrInitialize("slack"))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("Connection refused")))
            .thenReturn(Uni.createFrom().item(new McpSession("session-abc", McpWorkerConstants.PROTOCOL_VERSION)));

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("mcp:slack:send-message");
    }

    @Test
    void shutdown_transitionsToStopped() {
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "manual")
        ), 30);

        McpSession session = new McpSession("session-abc", McpWorkerConstants.PROTOCOL_VERSION);
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));
        when(sessionManager.shutdown()).thenReturn(Uni.createFrom().voidItem());

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);

        runtime.shutdown();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }

    // --- Helpers ---

    private HttpResponse<Buffer> mockResponse(int status) {
        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.statusMessage()).thenReturn("Status " + status);
        when(response.getHeader(anyString())).thenReturn(null);
        return response;
    }

    private HttpResponse<Buffer> mockJsonResponse(int status, String body) {
        HttpResponse<Buffer> response = mockResponse(status);
        when(response.bodyAsString()).thenReturn(body);
        when(response.body()).thenReturn(Buffer.buffer(body));
        return response;
    }
}
