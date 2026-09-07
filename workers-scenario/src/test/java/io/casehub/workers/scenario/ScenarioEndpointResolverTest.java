package io.casehub.workers.scenario;

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
import io.casehub.workers.scenario.ScenarioEndpointResolver.EndpointConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ScenarioEndpointResolverTest {

    private static final String TENANT_1 = "tenant-1";

    private static EndpointRegistry stubRegistry(String name, EndpointDescriptor descriptor) {
        return new EndpointRegistry() {
            @Override public void register(EndpointDescriptor endpoint) {}
            @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) {
                if (path.equals(Path.of("scenario", name))) return Optional.of(descriptor);
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

    private static EndpointDescriptor scenarioDescriptor(String name, Map<String, String> props) {
        return new EndpointDescriptor(
            Path.of("scenario", name),
            TENANT_1,
            EndpointType.WORKER,
            EndpointProtocol.SCENARIO,
            props,
            null,
            Set.of(EndpointCapability.DISPATCH)
        );
    }

    // --- Tier 2: Config tests ---

    @Test
    void singleEndpoint_resolvesCorrectly() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        ResolvedScenarioEndpoint resolved = resolver.resolve("scenario:onboard", TENANT_1);

        assertThat(resolved.name()).isEqualTo("onboard");
        assertThat(resolved.url()).isEqualTo("https://pages.example.com/graphql");
        assertThat(resolved.timeoutSeconds()).isEqualTo(600);
    }

    @Test
    void multipleEndpoints_eachResolvesIndependently() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages1.example.com/graphql", 600),
            new EndpointConfig("seed-data", "https://pages2.example.com/graphql", 120)
        ), 300);

        assertThat(resolver.resolve("scenario:onboard", TENANT_1).url())
            .isEqualTo("https://pages1.example.com/graphql");
        assertThat(resolver.resolve("scenario:seed-data", TENANT_1).url())
            .isEqualTo("https://pages2.example.com/graphql");
    }

    @Test
    void capabilities_returnsAllConfiguredTags() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600),
            new EndpointConfig("seed-data", "https://pages.example.com/graphql", 300)
        ), 300);

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
            "scenario:onboard", "scenario:seed-data"
        );
    }

    @Test
    void resolveUnknownTag_throws() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        assertThatThrownBy(() -> resolver.resolve("scenario:unknown", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("No route found");
    }

    @Test
    void resolveWrongPrefix_throws() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        assertThatThrownBy(() -> resolver.resolve("http:onboard", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("No route found");
    }

    @Test
    void timeoutFallsBackToDefault() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", -1)
        ), 300);

        assertThat(resolver.resolve("scenario:onboard", TENANT_1).timeoutSeconds()).isEqualTo(300);
    }

    @Test
    void blankUrl_throwsAtStartup() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();

        assertThatThrownBy(() -> resolver.initialize(List.of(
            new EndpointConfig("onboard", "", 600)
        ), 300))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("URL");
    }

    @Test
    void firstMatch_findsMatch() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        Optional<String> match = resolver.firstMatch(
            Set.of("scenario:onboard", "http:something"), TENANT_1);
        assertThat(match).hasValue("scenario:onboard");
    }

    @Test
    void firstMatch_noMatch_returnsEmpty() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        Optional<String> match = resolver.firstMatch(
            Set.of("http:something", "mcp:slack:send"), TENANT_1);
        assertThat(match).isEmpty();
    }

    @Test
    void canResolve_configuredTag_returnsTrue() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        assertThat(resolver.canResolve("scenario:onboard", TENANT_1)).isTrue();
        assertThat(resolver.canResolve("scenario:unknown", TENANT_1)).isFalse();
    }

    // --- Tier 3: EndpointRegistry tests ---

    @Test
    void tier3_registryHit_resolvesFromRegistry() {
        EndpointDescriptor descriptor = scenarioDescriptor("onboard", Map.of(
            EndpointPropertyKeys.URL, "https://registry-pages.example.com/graphql",
            "timeout-seconds", "120"
        ));
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300, stubRegistry("onboard", descriptor));

        ResolvedScenarioEndpoint resolved = resolver.resolve("scenario:onboard", TENANT_1);

        assertThat(resolved.name()).isEqualTo("onboard");
        assertThat(resolved.url()).isEqualTo("https://registry-pages.example.com/graphql");
        assertThat(resolved.timeoutSeconds()).isEqualTo(120);
    }

    @Test
    void tier3_registryMiss_throws() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300, emptyRegistry());

        assertThatThrownBy(() -> resolver.resolve("scenario:unknown", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("No route found");
    }

    @Test
    void tier2_winsOverTier3() {
        EndpointDescriptor descriptor = scenarioDescriptor("onboard", Map.of(
            EndpointPropertyKeys.URL, "https://registry.example.com/graphql"
        ));
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://config.example.com/graphql", 600)
        ), 300, stubRegistry("onboard", descriptor));

        ResolvedScenarioEndpoint resolved = resolver.resolve("scenario:onboard", TENANT_1);
        assertThat(resolved.url()).isEqualTo("https://config.example.com/graphql");
    }

    @Test
    void tier3_wrongProtocol_ignored() {
        EndpointDescriptor httpDescriptor = new EndpointDescriptor(
            Path.of("scenario", "onboard"),
            TENANT_1,
            EndpointType.WORKER,
            EndpointProtocol.HTTP,
            Map.of(EndpointPropertyKeys.URL, "https://http.example.com/scenario"),
            null,
            Set.of(EndpointCapability.DISPATCH)
        );
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300, stubRegistry("onboard", httpDescriptor));

        assertThatThrownBy(() -> resolver.resolve("scenario:onboard", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("No route found");
    }

    @Test
    void tier3_blankUrl_throws() {
        EndpointDescriptor descriptor = scenarioDescriptor("onboard", Map.of(
            EndpointPropertyKeys.URL, "   "
        ));
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300, stubRegistry("onboard", descriptor));

        assertThatThrownBy(() -> resolver.resolve("scenario:onboard", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("blank URL");
    }

    @Test
    void tier3_timeoutFallsBackToDefault() {
        EndpointDescriptor descriptor = scenarioDescriptor("onboard", Map.of(
            EndpointPropertyKeys.URL, "https://pages.example.com/graphql"
        ));
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 450, stubRegistry("onboard", descriptor));

        assertThat(resolver.resolve("scenario:onboard", TENANT_1).timeoutSeconds()).isEqualTo(450);
    }

    @Test
    void firstMatch_registryFallback() {
        EndpointDescriptor descriptor = scenarioDescriptor("onboard", Map.of(
            EndpointPropertyKeys.URL, "https://pages.example.com/graphql"
        ));
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300, stubRegistry("onboard", descriptor));

        Optional<String> match = resolver.firstMatch(
            Set.of("scenario:onboard", "http:something"), TENANT_1);
        assertThat(match).hasValue("scenario:onboard");
    }

    @Test
    void canResolve_registryFallback() {
        EndpointDescriptor descriptor = scenarioDescriptor("onboard", Map.of(
            EndpointPropertyKeys.URL, "https://pages.example.com/graphql"
        ));
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), 300, stubRegistry("onboard", descriptor));

        assertThat(resolver.canResolve("scenario:onboard", TENANT_1)).isTrue();
        assertThat(resolver.canResolve("scenario:unknown", TENANT_1)).isFalse();
    }

    @Test
    void nullRegistry_resolveFallsThrough() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(
            new EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        assertThat(resolver.resolve("scenario:onboard", TENANT_1).url())
            .isEqualTo("https://pages.example.com/graphql");

        assertThatThrownBy(() -> resolver.resolve("scenario:unknown", TENANT_1))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void parseScriptName_extractsNameFromTag() {
        assertThat(ScenarioEndpointResolver.parseScriptName("scenario:onboard")).isEqualTo("onboard");
        assertThat(ScenarioEndpointResolver.parseScriptName("scenario:")).isEmpty();
        assertThat(ScenarioEndpointResolver.parseScriptName("invalid")).isEmpty();
    }
}
