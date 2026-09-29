package io.casehub.workers.scenario;

import io.casehub.engine.common.spi.scheduler.WorkerBackend;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerFaultPublisher;
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
public class ScenarioWorkerBeans {

    @Inject Config config;
    @Inject EndpointRegistry endpointRegistry;

    @ConfigProperty(name = "casehub.workers.scenario.default-timeout-seconds", defaultValue = "600")
    int defaultTimeoutSeconds;

    @ConfigProperty(name = "casehub.workers.callback-base-url", defaultValue = "")
    String callbackBaseUrl;

    @Produces @ApplicationScoped
    ScenarioEndpointResolver endpointResolver() {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        List<ScenarioEndpointResolver.EndpointConfig> endpoints = loadFromConfig();
        resolver.initialize(endpoints, defaultTimeoutSeconds, endpointRegistry);
        return resolver;
    }

    @Produces @ApplicationScoped @WorkerBackend @Priority(10)
    ScenarioWorkerExecutionManager executionManager(ScenarioEndpointResolver resolver,
                                                     AsyncWorkerCompletionRegistry completionRegistry,
                                                     WorkerFaultPublisher faultPublisher) {
        return new ScenarioWorkerExecutionManager(resolver, completionRegistry, faultPublisher, callbackBaseUrl);
    }

    @Produces @ApplicationScoped
    ScenarioWorkerRuntime runtime(ScenarioEndpointResolver resolver) {
        return new ScenarioWorkerRuntime(resolver);
    }

    private List<ScenarioEndpointResolver.EndpointConfig> loadFromConfig() {
        if (config == null) return List.of();
        String prefix = "casehub.workers.scenario.endpoints.";
        Map<String, Map<String, String>> endpointProps = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String name = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        endpointProps.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(prop, value)
                    );
                }
            }
        }
        return endpointProps.entrySet().stream()
            .map(e -> buildEndpointConfig(e.getKey(), e.getValue()))
            .toList();
    }

    private ScenarioEndpointResolver.EndpointConfig buildEndpointConfig(String name, Map<String, String> props) {
        String url = props.get("url");
        int timeout = parseTimeout(props.get("timeout-seconds"));
        return new ScenarioEndpointResolver.EndpointConfig(name, url, timeout);
    }

    private static int parseTimeout(String value) {
        if (value == null || value.isBlank()) return -1;
        try { return Integer.parseInt(value); }
        catch (NumberFormatException e) { return -1; }
    }
}
