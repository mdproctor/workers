package io.casehub.workers.mcp;

import io.casehub.engine.common.spi.scheduler.WorkerBackend;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class McpWorkerBeans {

    @Inject Config config;
    @Inject EndpointRegistry endpointRegistry;

    @ConfigProperty(name = "casehub.workers.mcp.default-timeout-seconds", defaultValue = "30")
    int defaultTimeoutSeconds;

    @Produces @ApplicationScoped
    McpServerResolver serverResolver() {
        McpServerResolver resolver = new McpServerResolver();
        List<McpServerResolver.ServerConfig> servers = loadFromConfig();
        resolver.initialize(servers, defaultTimeoutSeconds, endpointRegistry);
        return resolver;
    }

    @Produces @ApplicationScoped @WorkerBackend @Priority(10)
    McpWorkerExecutionManager executionManager(McpServerResolver serverResolver,
                                                McpSessionProvider sessionProvider,
                                                WorkerFaultPublisher faultPublisher,
                                                WorkflowCompletionPublisher completionPublisher) {
        return new McpWorkerExecutionManager(serverResolver, sessionProvider, faultPublisher, completionPublisher);
    }

    private List<McpServerResolver.ServerConfig> loadFromConfig() {
        if (config == null) return List.of();
        String prefix = "casehub.workers.mcp.servers.";
        Map<String, Map<String, String>> serverProps = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String serverName = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        serverProps.computeIfAbsent(serverName, k -> new LinkedHashMap<>()).put(prop, value));
                }
            }
        }
        return serverProps.entrySet().stream()
            .map(entry -> buildServerConfig(entry.getKey(), entry.getValue()))
            .toList();
    }

    private McpServerResolver.ServerConfig buildServerConfig(String name, Map<String, String> props) {
        String url = props.get("url");
        String tools = props.getOrDefault("tools", "");
        int timeout = parseTimeout(props.get("timeout-seconds"));
        Map<String, String> headers = extractHeaders(props);
        String discovery = props.getOrDefault("discovery", "auto");
        return new McpServerResolver.ServerConfig(name, url, tools, timeout, headers, discovery);
    }

    private int parseTimeout(String value) {
        if (value == null || value.isBlank()) return -1;
        try { return Integer.parseInt(value); }
        catch (NumberFormatException e) { return -1; }
    }

    private Map<String, String> extractHeaders(Map<String, String> props) {
        Map<String, String> headers = new LinkedHashMap<>();
        String headerPrefix = "headers.";
        props.forEach((key, value) -> {
            if (key.startsWith(headerPrefix)) {
                headers.put(key.substring(headerPrefix.length()), value);
            }
        });
        return headers.isEmpty() ? Map.of() : Map.copyOf(headers);
    }
}
