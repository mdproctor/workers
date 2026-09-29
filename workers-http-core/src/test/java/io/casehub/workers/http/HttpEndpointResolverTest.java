package io.casehub.workers.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.platform.api.endpoints.EndpointCapability;
import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointQuery;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.endpoints.EndpointType;
import io.casehub.platform.api.path.Path;
import io.casehub.workers.common.WorkerProvisioningException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpEndpointResolverTest {

    private static final String TENANT_1 = "tenant-1";

    private HttpEndpointResolver resolver;

    private static final HttpWorkerRoute SEND_EMAIL_SPI = new HttpWorkerRoute() {
        @Override public String capabilityTag() { return "send-email"; }
        @Override public String url() { return "https://spi.example.com/send"; }
        @Override public String method() { return "PUT"; }
        @Override public ExchangeMode exchangeMode() { return ExchangeMode.ASYNC; }
        @Override public Map<String, String> headers() { return Map.of("X-SPI", "true"); }
        @Override public int timeoutSeconds() { return 60; }
    };

    private static final HttpWorkerRoute VALIDATE_SPI = new HttpWorkerRoute() {
        @Override public String capabilityTag() { return "validate-address"; }
        @Override public String url() { return "https://spi.example.com/validate"; }
        @Override public int timeoutSeconds() { return -1; }
    };

    private static final Map<String, Map<String, String>> CONFIG_ENDPOINTS = Map.of(
        "process-order", Map.of(
            "url", "https://orders.example.com/process",
            "method", "POST",
            "mode", "ASYNC",
            "timeout-seconds", "45",
            "headers.X-Api-Key", "secret-key"
        ),
        "send-email", Map.of(
            "url", "https://config.example.com/email",
            "method", "POST"
        ),
        "generate-report", Map.of(
            "url", "https://reports.example.com/generate"
        )
    );

    private static EndpointRegistry stubRegistry(String capabilityTag, EndpointDescriptor descriptor) {
        return new EndpointRegistry() {
            @Override public void register(EndpointDescriptor endpoint) {}
            @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) {
                if (path.equals(Path.of("http", capabilityTag))) return Optional.of(descriptor);
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

    private static EndpointDescriptor httpDescriptor(String capabilityTag, Map<String, String> props) {
        return new EndpointDescriptor(
            Path.of("http", capabilityTag),
            TENANT_1,
            EndpointType.WORKER,
            EndpointProtocol.HTTP,
            props,
            null,
            Set.of(EndpointCapability.DISPATCH)
        );
    }

    @BeforeEach
    void setUp() {
        resolver = new HttpEndpointResolver();
        resolver.initialize(
            List.of(SEND_EMAIL_SPI, VALIDATE_SPI),
            CONFIG_ENDPOINTS,
            30,
            emptyRegistry()
        );
    }

    @Test
    void tier1WinsOverTier2ForSameTag() {
        ResolvedEndpoint endpoint = resolver.resolve("send-email", TENANT_1);
        assertThat(endpoint.url()).isEqualTo("https://spi.example.com/send");
        assertThat(endpoint.method()).isEqualTo("PUT");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.ASYNC);
        assertThat(endpoint.headers()).containsEntry("X-SPI", "true");
        assertThat(endpoint.timeoutSeconds()).isEqualTo(60);
    }

    @Test
    void tier2ResolvesFromConfig() {
        ResolvedEndpoint endpoint = resolver.resolve("process-order", TENANT_1);
        assertThat(endpoint.url()).isEqualTo("https://orders.example.com/process");
        assertThat(endpoint.method()).isEqualTo("POST");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.ASYNC);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(45);
        assertThat(endpoint.headers()).containsEntry("X-Api-Key", "secret-key");
    }

    @Test
    void allTiersMerged() {
        Set<String> caps = resolver.capabilities();
        assertThat(caps).containsExactlyInAnyOrder(
            "send-email", "validate-address", "process-order", "generate-report"
        );
    }

    @Test
    void resolveUnknownTagThrows() {
        assertThatThrownBy(() -> resolver.resolve("nonexistent", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("nonexistent");
    }

    @Test
    void firstMatchReturnsFirstKnown() {
        assertThat(resolver.firstMatch(Set.of("unknown", "send-email"), TENANT_1))
            .isPresent()
            .hasValue("send-email");
    }

    @Test
    void firstMatchNoneKnownReturnsEmpty() {
        assertThat(resolver.firstMatch(Set.of("unknown-a", "unknown-b"), TENANT_1)).isEmpty();
    }

    @Test
    void capabilitiesReturnsAllResolvedTags() {
        assertThat(resolver.capabilities())
            .contains("send-email", "validate-address", "process-order", "generate-report");
    }

    @Test
    void configDefaultsToPostSyncWhenNotSpecified() {
        ResolvedEndpoint endpoint = resolver.resolve("generate-report", TENANT_1);
        assertThat(endpoint.method()).isEqualTo("POST");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.SYNC);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(30);
    }

    @Test
    void spiTimeoutMinusOneInheritsGlobalDefault() {
        ResolvedEndpoint endpoint = resolver.resolve("validate-address", TENANT_1);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(30);
    }

    @Test
    void spiWithExplicitTimeoutUsesItsOwnValue() {
        ResolvedEndpoint endpoint = resolver.resolve("send-email", TENANT_1);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(60);
    }

    @Test
    void configHeadersParsedFromDottedKeys() {
        ResolvedEndpoint endpoint = resolver.resolve("process-order", TENANT_1);
        assertThat(endpoint.headers()).containsEntry("X-Api-Key", "secret-key");
    }

    @Test
    void spiRouteEmptyHeadersPreserved() {
        ResolvedEndpoint endpoint = resolver.resolve("validate-address", TENANT_1);
        assertThat(endpoint.headers()).isEmpty();
    }

    @Test
    void initializeWithEmptySpiAndConfig() {
        HttpEndpointResolver empty = new HttpEndpointResolver();
        empty.initialize(List.of(), Map.of(), 30, emptyRegistry());
        assertThat(empty.capabilities()).isEmpty();
    }

    @Test
    void initializeWithNullConfig() {
        HttpEndpointResolver spiOnly = new HttpEndpointResolver();
        spiOnly.initialize(List.of(SEND_EMAIL_SPI), null, 30, emptyRegistry());
        assertThat(spiOnly.capabilities()).containsExactly("send-email");
    }

    @Test
    void firstMatchEmptyCapabilities() {
        assertThat(resolver.firstMatch(Set.of(), TENANT_1)).isEmpty();
    }

    @Test
    void capabilitiesReturnsUnmodifiableSet() {
        Set<String> caps = resolver.capabilities();
        assertThatThrownBy(() -> caps.add("should-fail"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void tier3_registryHit_resolvesFromRegistry() {
        EndpointDescriptor descriptor = httpDescriptor("payment-webhook", Map.of(
            "url", "https://registry.example.com/pay",
            "method", "PUT",
            "mode", "ASYNC",
            "timeout-seconds", "90",
            "headers.Authorization", "Bearer token-123"
        ));
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(), Map.of(), 30, stubRegistry("payment-webhook", descriptor));

        ResolvedEndpoint endpoint = r.resolve("payment-webhook", TENANT_1);

        assertThat(endpoint.url()).isEqualTo("https://registry.example.com/pay");
        assertThat(endpoint.method()).isEqualTo("PUT");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.ASYNC);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(90);
        assertThat(endpoint.headers()).containsEntry("Authorization", "Bearer token-123");
    }

    @Test
    void tier3_registryMiss_throws() {
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(), Map.of(), 30, emptyRegistry());

        assertThatThrownBy(() -> r.resolve("nonexistent", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("nonexistent");
    }

    @Test
    void tier1_winsOverTier3() {
        EndpointDescriptor descriptor = httpDescriptor("send-email", Map.of(
            "url", "https://registry.example.com/email"
        ));
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(SEND_EMAIL_SPI), Map.of(), 30, stubRegistry("send-email", descriptor));

        ResolvedEndpoint endpoint = r.resolve("send-email", TENANT_1);

        assertThat(endpoint.url()).isEqualTo("https://spi.example.com/send");
    }

    @Test
    void tier3_wrongProtocol_ignored() {
        EndpointDescriptor mcpDescriptor = new EndpointDescriptor(
            Path.of("http", "some-tool"),
            TENANT_1,
            EndpointType.WORKER,
            EndpointProtocol.MCP,
            Map.of("url", "https://mcp.example.com/tool"),
            null,
            Set.of(EndpointCapability.DISPATCH)
        );
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(), Map.of(), 30, stubRegistry("some-tool", mcpDescriptor));

        assertThatThrownBy(() -> r.resolve("some-tool", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("some-tool");
    }

    @Test
    void tier3_blankUrl_throws() {
        EndpointDescriptor descriptor = httpDescriptor("blank-url", Map.of(
            "url", "   "
        ));
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(), Map.of(), 30, stubRegistry("blank-url", descriptor));

        assertThatThrownBy(() -> r.resolve("blank-url", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("blank URL");
    }

    @Test
    void tier3_missingUrl_throws() {
        EndpointDescriptor descriptor = httpDescriptor("no-url", Map.of(
            "method", "POST"
        ));
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(), Map.of(), 30, stubRegistry("no-url", descriptor));

        assertThatThrownBy(() -> r.resolve("no-url", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("blank URL");
    }

    @Test
    void tier3_noOpRegistry_invisible() {
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(SEND_EMAIL_SPI), Map.of(), 30, emptyRegistry());

        ResolvedEndpoint endpoint = r.resolve("send-email", TENANT_1);
        assertThat(endpoint.url()).isEqualTo("https://spi.example.com/send");
        assertThat(r.capabilities()).containsExactly("send-email");
    }

    @Test
    void tier3_registryHit_defaultsApplied() {
        EndpointDescriptor descriptor = httpDescriptor("minimal", Map.of(
            "url", "https://registry.example.com/minimal"
        ));
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(), Map.of(), 30, stubRegistry("minimal", descriptor));

        ResolvedEndpoint endpoint = r.resolve("minimal", TENANT_1);
        assertThat(endpoint.url()).isEqualTo("https://registry.example.com/minimal");
        assertThat(endpoint.method()).isEqualTo("POST");
        assertThat(endpoint.mode()).isEqualTo(ExchangeMode.SYNC);
        assertThat(endpoint.timeoutSeconds()).isEqualTo(30);
        assertThat(endpoint.headers()).isEmpty();
    }

    @Test
    void firstMatch_checksRegistryAfterStaticSet() {
        EndpointDescriptor descriptor = httpDescriptor("registry-only-tag", Map.of(
            "url", "https://registry.example.com/only"
        ));
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(SEND_EMAIL_SPI), Map.of(), 30, stubRegistry("registry-only-tag", descriptor));

        assertThat(r.firstMatch(Set.of("registry-only-tag", "send-email"), TENANT_1))
            .isPresent()
            .hasValue("send-email");

        assertThat(r.firstMatch(Set.of("registry-only-tag"), TENANT_1))
            .isPresent()
            .hasValue("registry-only-tag");

        assertThat(r.firstMatch(Set.of("totally-unknown"), TENANT_1))
            .isEmpty();
    }

    @Test
    void firstMatch_registryWrongProtocol_notMatched() {
        EndpointDescriptor mcpDescriptor = new EndpointDescriptor(
            Path.of("http", "mcp-thing"),
            TENANT_1,
            EndpointType.WORKER,
            EndpointProtocol.MCP,
            Map.of("url", "https://mcp.example.com/thing"),
            null,
            Set.of(EndpointCapability.DISPATCH)
        );
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(), Map.of(), 30, stubRegistry("mcp-thing", mcpDescriptor));

        assertThat(r.firstMatch(Set.of("mcp-thing"), TENANT_1)).isEmpty();
    }

    @Test
    void nullRegistry_resolveFallsThrough() {
        HttpEndpointResolver r = new HttpEndpointResolver();
        r.initialize(List.of(SEND_EMAIL_SPI), Map.of(), 30, null);

        assertThat(r.resolve("send-email", TENANT_1).url())
            .isEqualTo("https://spi.example.com/send");

        assertThatThrownBy(() -> r.resolve("unknown", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class);
    }
}
