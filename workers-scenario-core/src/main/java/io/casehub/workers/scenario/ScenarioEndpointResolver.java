package io.casehub.workers.scenario;

import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.path.Path;
import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ScenarioEndpointResolver implements WorkerCapabilityResolver<ResolvedScenarioEndpoint> {

    private static final String TAG_PREFIX = "scenario:";

    public record EndpointConfig(String name, String url, int timeoutSeconds) {}

    private final Map<String, ResolvedScenarioEndpoint> endpointsByName = new LinkedHashMap<>();
    private EndpointRegistry registry;
    private int defaultTimeoutSeconds;

    public void initialize(List<EndpointConfig> endpoints, int defaultTimeout) {
        initialize(endpoints, defaultTimeout, null);
    }

    public void initialize(List<EndpointConfig> endpoints, int defaultTimeout, EndpointRegistry registry) {
        endpointsByName.clear();
        this.registry = registry;
        this.defaultTimeoutSeconds = defaultTimeout;

        for (EndpointConfig ep : endpoints) {
            validateConfig(ep);
            int timeout = ep.timeoutSeconds() == -1 ? defaultTimeout : ep.timeoutSeconds();
            endpointsByName.put(ep.name(), new ResolvedScenarioEndpoint(ep.name(), ep.url(), timeout));
        }
    }

    @Override
    public ResolvedScenarioEndpoint resolve(String capabilityTag, String tenancyId) {
        String name = parseScriptName(capabilityTag);
        if (name.isEmpty()) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }

        ResolvedScenarioEndpoint fromConfig = endpointsByName.get(name);
        if (fromConfig != null) {
            return fromConfig;
        }

        if (registry != null) {
            ResolvedScenarioEndpoint fromRegistry = resolveFromRegistry(name, tenancyId);
            if (fromRegistry != null) {
                return fromRegistry;
            }
        }

        throw WorkerProvisioningException.noRouteFound(capabilityTag);
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
        Optional<String> configMatch = capabilities.stream()
            .filter(cap -> {
                String name = parseScriptName(cap);
                return !name.isEmpty() && endpointsByName.containsKey(name);
            })
            .findFirst();
        if (configMatch.isPresent()) {
            return configMatch;
        }

        if (registry != null) {
            for (String cap : capabilities) {
                String name = parseScriptName(cap);
                if (!name.isEmpty()) {
                    Optional<EndpointDescriptor> descriptor =
                        registry.resolve(Path.of("scenario", name), tenancyId);
                    if (descriptor.isPresent() && descriptor.get().protocol() == EndpointProtocol.SCENARIO) {
                        return Optional.of(cap);
                    }
                }
            }
        }

        return Optional.empty();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(endpointsByName.keySet().stream()
            .map(name -> TAG_PREFIX + name)
            .toList());
    }

    @Override
    public boolean canResolve(String capabilityTag, String tenancyId) {
        String name = parseScriptName(capabilityTag);
        if (name.isEmpty()) return false;
        if (endpointsByName.containsKey(name)) return true;
        if (registry != null) {
            return registry.resolve(Path.of("scenario", name), tenancyId)
                .filter(d -> d.protocol() == EndpointProtocol.SCENARIO)
                .isPresent();
        }
        return false;
    }

    List<String> endpointNames() {
        return List.copyOf(endpointsByName.keySet());
    }

    public static String parseScriptName(String capabilityTag) {
        if (!capabilityTag.startsWith(TAG_PREFIX)) return "";
        return capabilityTag.substring(TAG_PREFIX.length());
    }

    private ResolvedScenarioEndpoint resolveFromRegistry(String name, String tenancyId) {
        return registry.resolve(Path.of("scenario", name), tenancyId)
            .filter(d -> d.protocol() == EndpointProtocol.SCENARIO)
            .map(d -> buildFromDescriptor(name, d))
            .orElse(null);
    }

    private ResolvedScenarioEndpoint buildFromDescriptor(String name, EndpointDescriptor descriptor) {
        Map<String, String> props = descriptor.properties();
        String url = props.get(EndpointPropertyKeys.URL);
        if (url == null || url.isBlank()) {
            throw new WorkerProvisioningException(
                "EndpointRegistry descriptor for scenario endpoint '" + name + "' has blank URL");
        }
        int timeout = parseTimeout(props.get("timeout-seconds"));
        if (timeout == -1) timeout = defaultTimeoutSeconds;
        return new ResolvedScenarioEndpoint(name, url, timeout);
    }

    private void validateConfig(EndpointConfig ep) {
        if (ep.url() == null || ep.url().isBlank()) {
            throw new WorkerProvisioningException(
                "Scenario endpoint '" + ep.name() + "' has blank URL — URL is required");
        }
    }

    private static int parseTimeout(String value) {
        if (value == null || value.isBlank()) return -1;
        try { return Integer.parseInt(value); }
        catch (NumberFormatException e) { return -1; }
    }
}
