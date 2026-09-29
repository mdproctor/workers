package io.casehub.workers.mcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkflowCompletionPublisher;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class McpWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(McpWorkerExecutionManager.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<Map<String, Object>>> LIST_MAP_TYPE = new TypeReference<>() {};

    private static final Set<Integer> PERMANENT_ERROR_CODES = Set.of(
        -32600, -32601, -32602, -32700
    );

    private final McpServerResolver serverResolver;
    private final McpSessionProvider sessionProvider;
    private final WorkerFaultPublisher faultPublisher;
    private final WorkflowCompletionPublisher completionPublisher;

    HttpClient httpClient = HttpClient.newHttpClient();

    public McpWorkerExecutionManager(McpServerResolver serverResolver,
                                      McpSessionProvider sessionProvider,
                                      WorkerFaultPublisher faultPublisher,
                                      WorkflowCompletionPublisher completionPublisher) {
        this.serverResolver = serverResolver;
        this.sessionProvider = sessionProvider;
        this.faultPublisher = faultPublisher;
        this.completionPublisher = completionPublisher;
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
        String capTag     = capability.name();
        String serverName = McpServerResolver.parseServerName(capTag);
        String toolName   = McpServerResolver.parseToolName(capTag);

        ResolvedMcpServer server;
        try {
            server = serverResolver.resolve(capTag, instance.tenancyId);
        } catch (Exception e) {
            faultPublisher.fault(buildCtx(instance, worker, capability, inputData, bindingName),
                                 capability, eventLogId,
                                 new PermanentFaultException(0, e.getMessage()));
            return;
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);

        try {
            McpSession session   = sessionProvider.getSession(serverName);
            long       requestId = session.nextRequestId();

            ObjectNode jsonRpcRequest = buildJsonRpcRequest(toolName, inputData, requestId);
            byte[] jsonBody = OBJECT_MAPPER.writeValueAsBytes(jsonRpcRequest);

            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(server.url()))
                .POST(HttpRequest.BodyPublishers.ofByteArray(jsonBody))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .header("MCP-Protocol-Version", session.protocolVersion())
                .timeout(Duration.ofSeconds(server.timeoutSeconds()));
            if (session.hasSessionId()) {
                builder.header("Mcp-Session-Id", session.sessionId());
            }
            server.headers().forEach(builder::header);

            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();

            if (status >= 200 && status < 300) {
                handleSuccessResponse(response, requestId, ctx);
                return;
            }
            if (status == 404) {
                if (session.hasSessionId()) {
                    sessionProvider.invalidate(serverName);
                    throw new RuntimeException("404 — MCP session expired, invalidated");
                } else {
                    throw new PermanentFaultException(404, "MCP endpoint not found");
                }
            }
            if (status == 429) {
                throw WorkerRetrySupport.parseRetryAfter(
                        response.headers().firstValue("Retry-After").orElse(null), status, "Too Many Requests");
            }
            if (status >= 400 && status < 500) {
                throw new PermanentFaultException(status, status + " Client Error");
            }
            throw new RuntimeException(status + " Server Error");
        } catch (Exception t) {
            faultPublisher.fault(ctx, capability, eventLogId, t);
        }
    }

    @Override
    public boolean supports(String capabilityName, String tenancyId) {
        return serverResolver.canResolve(capabilityName, tenancyId);
    }

    @Override
    public void schedulePersistedEvent(io.casehub.engine.common.internal.history.EventLog scheduledEventLog) {
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return 0;
    }

    private ObjectNode buildJsonRpcRequest(String toolName, Map<String, Object> inputData, long requestId) {
        ObjectNode root = OBJECT_MAPPER.createObjectNode();
        root.put("jsonrpc", "2.0");
        root.put("method", "tools/call");
        root.put("id", requestId);
        ObjectNode params = root.putObject("params");
        params.put("name", toolName);
        params.set("arguments", OBJECT_MAPPER.valueToTree(inputData));
        return root;
    }

    private void handleSuccessResponse(HttpResponse<String> response,
                                       long requestId,
                                       WorkerCorrelationContext ctx) {
        String contentType = response.headers().firstValue("Content-Type").orElse(null);
        String body        = response.body();

        JsonNode jsonRpc;
        try {
            if (contentType != null && contentType.startsWith("text/event-stream")) {
                jsonRpc = parseSSE(body, requestId);
                if (jsonRpc == null) {
                    throw new RuntimeException("No matching JSON-RPC response found in SSE stream");
                }
            } else {
                jsonRpc = OBJECT_MAPPER.readTree(body);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Malformed MCP response: " + e.getMessage());
        }

        handleJsonRpcResponse(jsonRpc, ctx);
    }

    private void handleJsonRpcResponse(JsonNode jsonRpc, WorkerCorrelationContext ctx) {
        if (jsonRpc.has("error")) {
            JsonNode error   = jsonRpc.get("error");
            int      code    = error.has("code") ? error.get("code").asInt() : 0;
            String   message = error.has("message") ? error.get("message").asText() : "Unknown error";

            if (PERMANENT_ERROR_CODES.contains(code)) {
                throw new PermanentFaultException(0, "JSON-RPC error " + code + ": " + message);
            }
            throw new RuntimeException("JSON-RPC error " + code + ": " + message);
        }

        JsonNode result = jsonRpc.get("result");
        if (result == null) {
            throw new RuntimeException("Malformed MCP response: missing 'result'");
        }

        if (result.has("isError") && result.get("isError").asBoolean()) {
            String text = extractContentText(result);
            throw new RuntimeException("MCP tool returned isError: " + text);
        }

        Map<String, Object> output;
        if (result.has("structuredContent") && result.get("structuredContent").isObject()) {
            output = OBJECT_MAPPER.convertValue(result.get("structuredContent"), MAP_TYPE);
        } else if (result.has("content")) {
            List<Map<String, Object>> contentList = OBJECT_MAPPER.convertValue(result.get("content"), LIST_MAP_TYPE);
            output = Map.of("content", contentList);
        } else {
            output = Map.of();
        }

        completionPublisher.complete(ctx, output);
    }

    private String extractContentText(JsonNode result) {
        if (!result.has("content") || !result.get("content").isArray()) {
            return "(no content)";
        }
        StringBuilder sb = new StringBuilder();
        for (JsonNode item : result.get("content")) {
            if (item.has("text")) {
                if (!sb.isEmpty()) sb.append("; ");
                sb.append(item.get("text").asText());
            }
        }
        return sb.isEmpty() ? "(no text)" : sb.toString();
    }

    private JsonNode parseSSE(String body, long expectedId) {
        if (body == null || body.isBlank()) return null;
        String[] events = body.split("\n\n");
        for (String event : events) {
            StringBuilder data = new StringBuilder();
            for (String line : event.split("\n")) {
                if (line.startsWith("data: ")) {
                    data.append(line.substring(6));
                } else if (line.startsWith("data:")) {
                    data.append(line.substring(5));
                }
            }
            if (data.isEmpty()) continue;
            try {
                JsonNode json = OBJECT_MAPPER.readTree(data.toString());
                if ((json.has("result") || json.has("error"))
                        && json.has("id") && json.get("id").asLong() == expectedId) {
                    return json;
                }
            } catch (Exception e) {
                LOG.finest("Skipping unparseable SSE event: " + e.getMessage());
            }
        }
        return null;
    }

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData,
                                              String bindingName) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.name(), capability.name(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId, bindingName);
    }
}
