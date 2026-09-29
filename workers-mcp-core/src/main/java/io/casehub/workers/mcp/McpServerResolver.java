package io.casehub.workers.mcp;

import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.path.Path;
import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

public class McpServerResolver implements WorkerCapabilityResolver<ResolvedMcpServer> {

    private static final Logger LOG = Logger.getLogger(McpServerResolver.class.getName());

    public record ServerConfig(String name, String url, String tools, int timeoutSeconds, Map<String, String> headers, String discovery) {}

    private final Map<String, ResolvedMcpServer> serversByName = new HashMap<>();
    private final Map<String, String> capabilityToServerName = new HashMap<>();
    private final Map<String, ServerConfig> configByName = new HashMap<>();
    private EndpointRegistry registry;
    private int defaultTimeoutSeconds;

    public void initialize(List<ServerConfig> servers, int defaultTimeout) {
        initialize(servers, defaultTimeout, null);
    }

    public void initialize(List<ServerConfig> servers, int defaultTimeout, EndpointRegistry registry) {
        serversByName.clear();
        capabilityToServerName.clear();
        configByName.clear();
        this.registry = registry;
        this.defaultTimeoutSeconds = defaultTimeout;

        for (ServerConfig config : servers) {
            validateServerConfig(config);
            configByName.put(config.name(), config);

            int timeout = config.timeoutSeconds() == -1 ? defaultTimeout : config.timeoutSeconds();
            Set<String> tools = parseTools(config.tools(), config.name());

            ResolvedMcpServer server = new ResolvedMcpServer(
                config.name(),
                config.url(),
                timeout,
                config.headers() == null ? Map.of() : Map.copyOf(config.headers()),
                tools
            );

            serversByName.put(config.name(), server);

            for (String tool : tools) {
                String capabilityTag = buildCapabilityTag(config.name(), tool);
                capabilityToServerName.put(capabilityTag, config.name());
            }
        }
    }

    @Override
    public ResolvedMcpServer resolve(String capabilityTag, String tenancyId) {
        String serverName = capabilityToServerName.get(capabilityTag);
        if (serverName != null) {
            return serversByName.get(serverName);
        }
        if (registry != null) {
            String parsedServer = parseServerName(capabilityTag);
            if (!parsedServer.isEmpty()) {
                ResolvedMcpServer fromRegistry = resolveFromRegistry(parsedServer, tenancyId);
                if (fromRegistry != null) {
                    return fromRegistry;
                }
            }
        }
        throw WorkerProvisioningException.noRouteFound(capabilityTag);
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
        Optional<String> staticMatch = capabilities.stream()
            .filter(capabilityToServerName::containsKey)
            .findFirst();
        if (staticMatch.isPresent()) {
            return staticMatch;
        }
        if (registry != null) {
            for (String cap : capabilities) {
                String parsedServer = parseServerName(cap);
                if (!parsedServer.isEmpty()) {
                    Optional<EndpointDescriptor> descriptor =
                        registry.resolve(Path.of("mcp", parsedServer), tenancyId);
                    if (descriptor.isPresent() && descriptor.get().protocol() == EndpointProtocol.MCP) {
                        return Optional.of(cap);
                    }
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(capabilityToServerName.keySet());
    }

    @Override
    public boolean canResolve(String capabilityTag, String tenancyId) {
        if (capabilityToServerName.containsKey(capabilityTag)) {
            return true;
        }
        if (registry != null) {
            String parsedServer = parseServerName(capabilityTag);
            if (!parsedServer.isEmpty()) {
                return registry.resolve(Path.of("mcp", parsedServer), tenancyId)
                    .filter(d -> d.protocol() == EndpointProtocol.MCP)
                    .isPresent();
            }
        }
        return false;
    }

    public ResolvedMcpServer serverByName(String name) {
        ResolvedMcpServer server = serversByName.get(name);
        if (server == null) {
            throw new WorkerProvisioningException("No MCP server found with name: " + name);
        }
        return server;
    }

    public boolean isDiscoveryEnabled(String serverName) {
        ServerConfig config = configByName.get(serverName);
        if (config == null) return false;
        String mode = config.discovery();
        return mode == null || mode.isBlank() || "auto".equalsIgnoreCase(mode);
    }

    public List<String> serverNames() {
        return List.copyOf(serversByName.keySet());
    }

    public void registerDiscoveredTools(String serverName, Set<String> discoveredToolNames) {
        ResolvedMcpServer existing = serversByName.get(serverName);
        if (existing == null) {
            throw new WorkerProvisioningException("No MCP server found with name: " + serverName);
        }

        ServerConfig config = configByName.get(serverName);
        Set<String> configTools = parseTools(config.tools(), serverName);
        Set<String> finalTools;

        if (configTools.isEmpty()) {
            finalTools = Set.copyOf(discoveredToolNames);
        } else {
            for (String configTool : configTools) {
                if (!discoveredToolNames.contains(configTool)) {
                    LOG.warning("MCP server '" + serverName + "': config-declared tool '" + configTool + "' not found in tools/list response");
                }
            }
            finalTools = configTools;
        }

        Set<String> oldTags = new HashSet<>();
        capabilityToServerName.forEach((tag, name) -> {
            if (name.equals(serverName)) oldTags.add(tag);
        });
        oldTags.forEach(capabilityToServerName::remove);

        ResolvedMcpServer updated = new ResolvedMcpServer(
            existing.name(), existing.url(), existing.timeoutSeconds(),
            existing.headers(), finalTools
        );
        serversByName.put(serverName, updated);

        for (String tool : finalTools) {
            capabilityToServerName.put(buildCapabilityTag(serverName, tool), serverName);
        }
    }

    public static String parseServerName(String capabilityTag) {
        String[] parts = capabilityTag.split(":", 3);
        return parts.length >= 2 ? parts[1] : "";
    }

    public static String parseToolName(String capabilityTag) {
        String[] parts = capabilityTag.split(":", 3);
        return parts.length >= 3 ? parts[2] : "";
    }

    private String buildCapabilityTag(String serverName, String tool) {
        return "mcp:" + serverName + ":" + tool;
    }

    private void validateServerConfig(ServerConfig config) {
        if (config.url() == null || config.url().isBlank()) {
            throw new WorkerProvisioningException(
                "MCP server '" + config.name() + "' has blank URL — URL is required"
            );
        }
    }

    private Set<String> parseTools(String toolsStr, String serverName) {
        if (toolsStr == null || toolsStr.isBlank()) {
            return Set.of();
        }

        Set<String> tools = new HashSet<>();
        String[] parts = toolsStr.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                if (tools.contains(trimmed)) {
                    throw new WorkerProvisioningException(
                        "MCP server '" + serverName + "' has duplicate tool: " + trimmed
                    );
                }
                tools.add(trimmed);
            }
        }
        return Set.copyOf(tools);
    }

    private ResolvedMcpServer resolveFromRegistry(String serverName, String tenancyId) {
        return registry.resolve(Path.of("mcp", serverName), tenancyId)
            .filter(d -> d.protocol() == EndpointProtocol.MCP)
            .map(d -> buildFromDescriptor(serverName, d))
            .orElse(null);
    }

    private ResolvedMcpServer buildFromDescriptor(String serverName, EndpointDescriptor descriptor) {
        Map<String, String> props = descriptor.properties();
        String url = props.get(EndpointPropertyKeys.URL);
        if (url == null || url.isBlank()) {
            throw new WorkerProvisioningException(
                "EndpointRegistry descriptor for MCP server '" + serverName + "' has blank URL");
        }
        int timeout = parseTimeout(props.get("timeout-seconds"));
        if (timeout == -1) timeout = defaultTimeoutSeconds;
        Map<String, String> headers = extractHeaders(props);
        Set<String> tools = parseTools(props.getOrDefault("tools", ""), serverName);
        return new ResolvedMcpServer(serverName, url, timeout, headers, tools);
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
