package io.casehub.workers.http;

import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.path.Path;
import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HttpEndpointResolver implements WorkerCapabilityResolver<ResolvedEndpoint> {

    private final Map<String, ResolvedEndpoint> resolvedEndpoints = new HashMap<>();
    private EndpointRegistry registry;
    private int defaultTimeoutSeconds;

    public void initialize(List<HttpWorkerRoute> spiRouteList,
                           Map<String, Map<String, String>> configEndpoints,
                           int defaultTimeout,
                           EndpointRegistry registry) {
        resolvedEndpoints.clear();
        this.registry = registry;
        this.defaultTimeoutSeconds = defaultTimeout;

        for (HttpWorkerRoute route : spiRouteList) {
            int timeout = route.timeoutSeconds() == -1 ? defaultTimeout : route.timeoutSeconds();
            resolvedEndpoints.put(route.capabilityTag(), new ResolvedEndpoint(
                route.url(),
                route.method(),
                route.exchangeMode(),
                route.headers(),
                timeout
            ));
        }

        if (configEndpoints != null) {
            configEndpoints.forEach((tag, props) -> {
                resolvedEndpoints.putIfAbsent(tag, buildFromConfig(props, defaultTimeout));
            });
        }
    }

    @Override
    public ResolvedEndpoint resolve(String capabilityTag, String tenancyId) {
        ResolvedEndpoint endpoint = resolvedEndpoints.get(capabilityTag);
        if (endpoint != null) {
            return endpoint;
        }
        if (registry != null) {
            endpoint = resolveFromRegistry(capabilityTag, tenancyId);
            if (endpoint != null) {
                return endpoint;
            }
        }
        throw WorkerProvisioningException.noRouteFound(capabilityTag);
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
        Optional<String> staticMatch = capabilities.stream()
            .filter(resolvedEndpoints::containsKey)
            .findFirst();
        if (staticMatch.isPresent()) {
            return staticMatch;
        }
        if (registry != null) {
            for (String cap : capabilities) {
                if (resolveFromRegistry(cap, tenancyId) != null) {
                    return Optional.of(cap);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(resolvedEndpoints.keySet());
    }

    @Override
    public boolean canResolve(String capabilityTag, String tenancyId) {
        if (resolvedEndpoints.containsKey(capabilityTag)) {
            return true;
        }
        if (registry != null) {
            return registry.resolve(Path.of("http", capabilityTag), tenancyId)
                .filter(d -> d.protocol() == EndpointProtocol.HTTP)
                .isPresent();
        }
        return false;
    }

    private ResolvedEndpoint resolveFromRegistry(String capabilityTag, String tenancyId) {
        return registry.resolve(Path.of("http", capabilityTag), tenancyId)
            .filter(d -> d.protocol() == EndpointProtocol.HTTP)
            .map(this::buildFromDescriptor)
            .orElse(null);
    }

    private ResolvedEndpoint buildFromDescriptor(EndpointDescriptor descriptor) {
        Map<String, String> props = descriptor.properties();
        String url = props.get(EndpointPropertyKeys.URL);
        if (url == null || url.isBlank()) {
            throw new WorkerProvisioningException(
                "EndpointRegistry descriptor for " + descriptor.path().value()
                + " has blank URL");
        }
        String method = props.getOrDefault("method", "POST");
        ExchangeMode mode = parseMode(props.getOrDefault("mode", "SYNC"));
        int timeout = parseTimeout(props.get("timeout-seconds"), defaultTimeoutSeconds);
        Map<String, String> headers = extractHeaders(props);
        return new ResolvedEndpoint(url, method, mode, headers, timeout);
    }

    private ResolvedEndpoint buildFromConfig(Map<String, String> props, int defaultTimeout) {
        String url = props.get("url");
        String method = props.getOrDefault("method", "POST");
        ExchangeMode mode = parseMode(props.getOrDefault("mode", "SYNC"));
        int timeout = parseTimeout(props.get("timeout-seconds"), defaultTimeout);
        Map<String, String> headers = extractHeaders(props);
        return new ResolvedEndpoint(url, method, mode, headers, timeout);
    }

    private ExchangeMode parseMode(String value) {
        try {
            return ExchangeMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ExchangeMode.SYNC;
        }
    }

    private int parseTimeout(String value, int defaultTimeout) {
        if (value == null || value.isBlank()) {
            return defaultTimeout;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultTimeout;
        }
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
