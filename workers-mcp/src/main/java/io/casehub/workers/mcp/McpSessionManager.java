package io.casehub.workers.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.workers.common.PermanentFaultException;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class McpSessionManager implements McpSessionProvider {

    private static final Logger LOG = Logger.getLogger(McpSessionManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Inject McpServerResolver serverResolver;
    @Inject io.vertx.mutiny.core.Vertx vertx;

    WebClient webClient;

    private final ConcurrentHashMap<String, Uni<McpSession>> sessions = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        this.webClient = WebClient.create(vertx);
    }

    /**
     * Returns a cached session for the named server, or triggers initialization
     * if none exists. Concurrent callers for the same server share a single
     * initialization Uni via {@code memoize().indefinitely()}.
     */
    public Uni<McpSession> getOrInitialize(String serverName) {
        return sessions.computeIfAbsent(serverName,
            k -> performInitialization(k)
                .onFailure().invoke(() -> sessions.remove(k))
                .memoize().indefinitely());
    }

    /**
     * Removes the cached session for the named server. The next call to
     * {@link #getOrInitialize(String)} will trigger re-initialization.
     */
    public void invalidate(String serverName) {
        sessions.remove(serverName);
    }

    @Override
    public McpSession getSession(String serverName) {
        return getOrInitialize(serverName).await().indefinitely();
    }


    public Uni<Void> shutdown() {
        return Uni.createFrom().item(() -> {
            for (Map.Entry<String, Uni<McpSession>> entry : sessions.entrySet()) {
                try {
                    McpSession session = entry.getValue()
                        .await().atMost(java.time.Duration.ofMillis(100));
                    if (session != null && session.hasSessionId()) {
                        ResolvedMcpServer server = serverResolver.serverByName(entry.getKey());
                        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.DELETE, server.url());
                        request.putHeader("Mcp-Session-Id", session.sessionId());
                        request.send().subscribe().with(
                            resp -> LOG.debugf("Shutdown DELETE for %s: %d", entry.getKey(), resp.statusCode()),
                            err -> LOG.debugf("Shutdown DELETE for %s failed: %s", entry.getKey(), err.getMessage())
                        );
                    }
                } catch (Exception e) {
                    LOG.debugf("Skipping shutdown for %s: %s", entry.getKey(), e.getMessage());
                }
            }
            return null;
        }).replaceWithVoid();
    }

    private Uni<McpSession> performInitialization(String serverName) {
        ResolvedMcpServer server = serverResolver.serverByName(serverName);

        ObjectNode initBody = buildInitializeRequest();

        return sendInitialize(server, initBody)
            .flatMap(response -> parseInitializeResponse(response, server));
    }

    private ObjectNode buildInitializeRequest() {
        ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("jsonrpc", "2.0");
        root.put("id", 1);
        root.put("method", "initialize");
        ObjectNode params = root.putObject("params");
        params.put("protocolVersion", McpWorkerConstants.PROTOCOL_VERSION);
        params.putObject("capabilities");
        ObjectNode clientInfo = params.putObject("clientInfo");
        clientInfo.put("name", McpWorkerConstants.CLIENT_NAME);
        clientInfo.put("version", McpWorkerConstants.CLIENT_VERSION);
        return root;
    }

    private Uni<HttpResponse<Buffer>> sendInitialize(ResolvedMcpServer server, ObjectNode body) {
        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, server.url());
        request.timeout(server.timeoutSeconds() * 1000L);
        request.putHeader("Content-Type", "application/json");
        request.putHeader("Accept", "application/json, text/event-stream");
        server.headers().forEach(request::putHeader);

        return request.sendJson(body);
    }

    private Uni<McpSession> parseInitializeResponse(HttpResponse<Buffer> response, ResolvedMcpServer server) {
        int status = response.statusCode();

        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status, status + " " + response.statusMessage());
        }
        if (status >= 500) {
            throw new RuntimeException(status + " " + response.statusMessage());
        }
        if (status < 200 || status >= 300) {
            throw new RuntimeException("Unexpected status " + status + " " + response.statusMessage());
        }

        String bodyStr = response.bodyAsString();
        JsonNode json;
        try {
            json = OBJECT_MAPPER.readTree(bodyStr);
        } catch (Exception e) {
            throw new PermanentFaultException(0, "Malformed JSON in initialize response: " + e.getMessage());
        }

        if (json.has("error")) {
            JsonNode error = json.get("error");
            String message = error != null ? error.path("message").asText("Unknown error") : "Unknown error";
            throw new PermanentFaultException(0, "JSON-RPC error: " + message);
        }

        JsonNode result = json.get("result");
        if (result == null) {
            throw new PermanentFaultException(0, "Missing 'result' in initialize response");
        }

        String serverProtocolVersion = result.path("protocolVersion").asText("");
        if (!McpWorkerConstants.PROTOCOL_VERSION.equals(serverProtocolVersion)) {
            throw new PermanentFaultException(0,
                "Protocol version mismatch: expected " + McpWorkerConstants.PROTOCOL_VERSION
                    + " but server returned " + serverProtocolVersion);
        }

        String sessionId = response.getHeader("Mcp-Session-Id");
        McpSession session = new McpSession(sessionId, serverProtocolVersion);

        // Send initialized notification
        return sendInitializedNotification(server, session)
            .onItem().transform(v -> session);
    }

    private Uni<Void> sendInitializedNotification(ResolvedMcpServer server, McpSession session) {
        ObjectNode notificationBody = OBJECT_MAPPER.createObjectNode();
        notificationBody.put("jsonrpc", "2.0");
        notificationBody.put("method", "notifications/initialized");

        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, server.url());
        request.timeout(server.timeoutSeconds() * 1000L);
        request.putHeader("Content-Type", "application/json");
        request.putHeader("Accept", "application/json, text/event-stream");
        request.putHeader("MCP-Protocol-Version", session.protocolVersion());
        if (session.hasSessionId()) {
            request.putHeader("Mcp-Session-Id", session.sessionId());
        }
        server.headers().forEach(request::putHeader);

        return request.sendJson(notificationBody)
            .onItem().invoke(response -> {
                if (response.statusCode() != 202) {
                    LOG.warnf("Server %s responded %d to initialized notification (expected 202)",
                        server.name(), response.statusCode());
                }
            })
            .onFailure().invoke(err ->
                LOG.warnf("Failed to send initialized notification to %s: %s",
                    server.name(), err.getMessage()))
            .replaceWith(Uni.createFrom().<Void>voidItem())
            .onFailure().recoverWithUni(err -> Uni.createFrom().voidItem());
    }
}
