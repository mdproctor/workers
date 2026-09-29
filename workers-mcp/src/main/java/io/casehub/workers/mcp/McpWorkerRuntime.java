package io.casehub.workers.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class McpWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(McpWorkerRuntime.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Inject McpServerResolver serverResolver;
    @Inject McpSessionManager sessionManager;
    @Inject io.vertx.mutiny.core.Vertx vertx;

    WebClient webClient;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @PostConstruct
    void init() {
        this.webClient = WebClient.create(vertx);
    }

    @Override
    public String workerType() {
        return McpWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public void initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) { return; }

        List<String> serverNames = serverResolver.serverNames();

        if (serverNames.isEmpty()) {
            LOG.warn("No MCP servers configured — status FAULTED");
            status = WorkerRuntimeStatus.FAULTED;
            return;
        }

        List<Uni<ServerInitResult>> initUnis = new ArrayList<>();
        for (String serverName : serverNames) {
            initUnis.add(initializeServer(serverName));
        }

        List<ServerInitResult> results = Uni.join().all(initUnis).andFailFast()
            .await().indefinitely();
        processResults(results);
    }

    @Override
    public void shutdown() {
        try {
            sessionManager.shutdown().await().indefinitely();
            status = WorkerRuntimeStatus.STOPPED;
        } catch (Exception err) {
            LOG.warnf("Error during MCP shutdown: %s", err.getMessage());
            status = WorkerRuntimeStatus.STOPPED;
        }
    }

    @Override
    public Set<String> capabilities() {
        return serverResolver.capabilities();
    }

    private Uni<ServerInitResult> initializeServer(String serverName) {
        return sessionManager.getOrInitialize(serverName)
            .flatMap(session -> {
                if (serverResolver.isDiscoveryEnabled(serverName)) {
                    return discoverTools(serverName, session)
                        .onItem().transform(tools ->
                            ServerInitResult.success(serverName, session, tools));
                }
                return Uni.createFrom().item(
                    ServerInitResult.success(serverName, session, Set.of()));
            })
            .onFailure().recoverWithItem(err -> {
                LOG.warnf("MCP server '%s' initialization failed: %s", serverName, err.getMessage());
                return ServerInitResult.failure(serverName, err);
            });
    }

    private Uni<Set<String>> discoverTools(String serverName, McpSession session) {
        ResolvedMcpServer server = serverResolver.serverByName(serverName);
        long requestId = session.nextRequestId();

        ObjectNode jsonRpcRequest = OBJECT_MAPPER.createObjectNode();
        jsonRpcRequest.put("jsonrpc", "2.0");
        jsonRpcRequest.put("id", requestId);
        jsonRpcRequest.put("method", "tools/list");

        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, server.url());
        request.putHeader("Content-Type", "application/json");
        request.putHeader("Accept", "application/json, text/event-stream");
        request.putHeader("MCP-Protocol-Version", session.protocolVersion());
        if (session.hasSessionId()) {
            request.putHeader("Mcp-Session-Id", session.sessionId());
        }
        server.headers().forEach(request::putHeader);
        request.timeout(server.timeoutSeconds() * 1000L);

        return request.sendJson(jsonRpcRequest)
            .onItem().transform(response -> parseToolsListResponse(response, serverName))
            .onFailure().recoverWithItem(err -> {
                LOG.warnf("MCP server '%s': tools/list failed, falling back to config: %s",
                    serverName, err.getMessage());
                return Set.of();
            });
    }

    private Set<String> parseToolsListResponse(HttpResponse<Buffer> response, String serverName) {
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            LOG.warnf("MCP server '%s': tools/list returned status %d, falling back to config",
                serverName, response.statusCode());
            return Set.of();
        }

        String body = response.bodyAsString();
        try {
            JsonNode json = OBJECT_MAPPER.readTree(body);

            if (json.has("error")) {
                String message = json.path("error").path("message").asText("Unknown error");
                LOG.warnf("MCP server '%s': tools/list returned JSON-RPC error: %s, falling back to config",
                    serverName, message);
                return Set.of();
            }

            JsonNode tools = json.path("result").path("tools");
            if (!tools.isArray()) {
                LOG.warnf("MCP server '%s': tools/list response missing result.tools array, falling back to config",
                    serverName);
                return Set.of();
            }

            Set<String> toolNames = new HashSet<>();
            for (JsonNode tool : tools) {
                String name = tool.path("name").asText(null);
                if (name != null && !name.isBlank()) {
                    toolNames.add(name);
                }
            }
            return Set.copyOf(toolNames);
        } catch (Exception e) {
            LOG.warnf("MCP server '%s': failed to parse tools/list response: %s, falling back to config",
                serverName, e.getMessage());
            return Set.of();
        }
    }

    private void processResults(List<ServerInitResult> results) {
        boolean anySuccess = false;

        for (ServerInitResult result : results) {
            if (result.success()) {
                anySuccess = true;
                if (!result.discoveredTools().isEmpty()) {
                    serverResolver.registerDiscoveredTools(result.serverName(), result.discoveredTools());
                }
                LOG.infof("MCP server '%s' initialized successfully", result.serverName());
            } else {
                LOG.warnf("MCP server '%s' failed to initialize: %s",
                    result.serverName(),
                    result.error() != null ? result.error().getMessage() : "unknown error");
            }
        }

        status = anySuccess ? WorkerRuntimeStatus.RUNNING : WorkerRuntimeStatus.FAULTED;
    }
}
