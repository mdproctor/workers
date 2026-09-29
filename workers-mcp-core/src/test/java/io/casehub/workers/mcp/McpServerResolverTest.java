package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.platform.api.endpoints.EndpointCapability;
import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointQuery;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.endpoints.EndpointType;
import io.casehub.platform.api.path.Path;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.mcp.McpServerResolver.ServerConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class McpServerResolverTest {

    private static final String TENANT_1 = "tenant-1";

    private static EndpointRegistry stubMcpRegistry(String serverName, EndpointDescriptor descriptor) {
        return new EndpointRegistry() {
            @Override public void register(EndpointDescriptor endpoint) {}
            @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) {
                if (path.equals(Path.of("mcp", serverName))) return Optional.of(descriptor);
                return Optional.empty();
            }
            @Override public List<EndpointDescriptor> discover(EndpointQuery query) { return List.of(); }
            @Override public void deregister(Path path, String tenancyId) {}
        };
    }

    private static EndpointRegistry emptyRegistry() {
        return new EndpointRegistry() {
            @Override public void register(EndpointDescriptor endpoint) {}
            @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) { return Optional.empty(); }
            @Override public List<EndpointDescriptor> discover(EndpointQuery query) { return List.of(); }
            @Override public void deregister(Path path, String tenancyId) {}
        };
    }

    private static EndpointDescriptor mcpDescriptor(String serverName, Map<String, String> props) {
        return new EndpointDescriptor(Path.of("mcp", serverName), TENANT_1, EndpointType.WORKER, EndpointProtocol.MCP, props, null, Set.of(EndpointCapability.DISPATCH));
    }

    @Test void singleServerWithTwoTools_buildsTwoCapabilities() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message,list-channels", 30, Map.of(), "auto")), 30);
        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("mcp:slack:send-message", "mcp:slack:list-channels");
    }

    @Test void multipleServers_combinedCapabilities() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto"),
            new ServerConfig("jira", "https://jira.internal/mcp", "create-issue,search", 60, Map.of(), "auto")), 30);
        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("mcp:slack:send-message", "mcp:jira:create-issue", "mcp:jira:search");
    }

    @Test void resolveWithValidTag_returnsCorrectServer() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of("Authorization", "Bearer xxx"), "auto")), 30);
        ResolvedMcpServer resolved = resolver.resolve("mcp:slack:send-message", TENANT_1);
        assertThat(resolved.name()).isEqualTo("slack");
        assertThat(resolved.url()).isEqualTo("https://slack.internal/mcp");
        assertThat(resolved.headers()).containsEntry("Authorization", "Bearer xxx");
    }

    @Test void multipleTagsToSameServer_returnsSameInstance() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message,list-channels", 30, Map.of(), "auto")), 30);
        assertThat(resolver.resolve("mcp:slack:send-message", TENANT_1)).isSameAs(resolver.resolve("mcp:slack:list-channels", TENANT_1));
    }

    @Test void resolveWithUnknownServer_throws() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        assertThatThrownBy(() -> resolver.resolve("mcp:unknown:send-message", TENANT_1)).isInstanceOf(WorkerProvisioningException.class);
    }

    @Test void resolveWithKnownServerButUnlistedTool_throws() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        assertThatThrownBy(() -> resolver.resolve("mcp:slack:delete-message", TENANT_1)).isInstanceOf(WorkerProvisioningException.class);
    }

    @Test void timeoutFallsBackToGlobalDefault() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", -1, Map.of(), "auto")), 45);
        assertThat(resolver.resolve("mcp:slack:send-message", TENANT_1).timeoutSeconds()).isEqualTo(45);
    }

    @Test void blankUrl_throwsAtStartup() {
        McpServerResolver resolver = new McpServerResolver();
        assertThatThrownBy(() -> resolver.initialize(List.of(new ServerConfig("slack", "", "send-message", 30, Map.of(), "auto")), 30))
            .isInstanceOf(WorkerProvisioningException.class).hasMessageContaining("URL");
    }

    @Test void toolsParsing_duplicateToolNames_throws() {
        McpServerResolver resolver = new McpServerResolver();
        assertThatThrownBy(() -> resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message,send-message", 30, Map.of(), "auto")), 30))
            .isInstanceOf(WorkerProvisioningException.class).hasMessageContaining("duplicate");
    }

    @Test void emptyTools_noCapabilities() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")), 30);
        assertThat(resolver.capabilities()).isEmpty();
    }

    @Test void firstMatch_findsMatch() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        assertThat(resolver.firstMatch(Set.of("mcp:slack:send-message", "http:send-email"), TENANT_1)).hasValue("mcp:slack:send-message");
    }

    @Test void firstMatch_noMatch_returnsEmpty() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        assertThat(resolver.firstMatch(Set.of("http:send-email"), TENANT_1)).isEmpty();
    }

    @Test void parseServerName_extractsServerFromTag() {
        assertThat(McpServerResolver.parseServerName("mcp:slack:send-message")).isEqualTo("slack");
    }

    @Test void parseToolName_extractsToolFromTag() {
        assertThat(McpServerResolver.parseToolName("mcp:slack:send-message")).isEqualTo("send-message");
    }

    @Test void serverByName_returnsCorrectServer() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        assertThat(resolver.serverByName("slack").name()).isEqualTo("slack");
    }

    @Test void serverByName_unknownServer_throws() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        assertThatThrownBy(() -> resolver.serverByName("jira")).isInstanceOf(WorkerProvisioningException.class);
    }

    @Test void registerDiscoveredTools_fullDiscovery_registersAllTools() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")), 30);
        resolver.registerDiscoveredTools("slack", Set.of("send-message", "list-channels"));
        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("mcp:slack:send-message", "mcp:slack:list-channels");
    }

    @Test void registerDiscoveredTools_withAllowlist_registersOnlyConfigTools() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        resolver.registerDiscoveredTools("slack", Set.of("send-message", "list-channels", "delete-message"));
        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("mcp:slack:send-message");
    }

    @Test void isDiscoveryEnabled_autoMode_returnsTrue() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")), 30);
        assertThat(resolver.isDiscoveryEnabled("slack")).isTrue();
    }

    @Test void isDiscoveryEnabled_manualMode_returnsFalse() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "manual")), 30);
        assertThat(resolver.isDiscoveryEnabled("slack")).isFalse();
    }

    @Test void tier3_registryHit_resolvesServerFromRegistry() {
        EndpointDescriptor descriptor = mcpDescriptor("slack", Map.of(EndpointPropertyKeys.URL, "https://slack.internal/mcp", "timeout-seconds", "60", "tools", "send-message,list-channels", "headers.Authorization", "Bearer token"));
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(), 30, stubMcpRegistry("slack", descriptor));
        ResolvedMcpServer resolved = resolver.resolve("mcp:slack:send-message", TENANT_1);
        assertThat(resolved.url()).isEqualTo("https://slack.internal/mcp");
        assertThat(resolved.timeoutSeconds()).isEqualTo(60);
    }

    @Test void tier3_registryMiss_throws() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(), 30, emptyRegistry());
        assertThatThrownBy(() -> resolver.resolve("mcp:unknown:some-tool", TENANT_1)).isInstanceOf(WorkerProvisioningException.class);
    }

    @Test void tier1_winsOverTier3() {
        EndpointDescriptor descriptor = mcpDescriptor("slack", Map.of(EndpointPropertyKeys.URL, "https://registry.example.com/slack-mcp"));
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://config.internal/mcp", "send-message", 30, Map.of(), "auto")), 30, stubMcpRegistry("slack", descriptor));
        assertThat(resolver.resolve("mcp:slack:send-message", TENANT_1).url()).isEqualTo("https://config.internal/mcp");
    }

    @Test void nullRegistry_resolveFallsThrough() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")), 30);
        assertThat(resolver.resolve("mcp:slack:send-message", TENANT_1).url()).isEqualTo("https://slack.internal/mcp");
        assertThatThrownBy(() -> resolver.resolve("mcp:unknown:tool", TENANT_1)).isInstanceOf(WorkerProvisioningException.class);
    }
}
