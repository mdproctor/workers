package io.casehub.workers.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.engine.common.spi.scheduler.WorkerBackend;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class HttpWorkerBeans {

    @Inject @Any
    Instance<HttpWorkerRoute> spiRoutes;

    @Inject
    Config config;

    @Inject
    EndpointRegistry endpointRegistry;

    @ConfigProperty(name = "casehub.workers.http.default-timeout-seconds", defaultValue = "30")
    int defaultTimeoutSeconds;

    @ConfigProperty(name = "casehub.workers.async.timeout-minutes", defaultValue = "60")
    int asyncTimeoutMinutes;

    @Produces @ApplicationScoped
    HttpEndpointResolver endpointResolver() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        List<HttpWorkerRoute> routes = List.of();
        if (spiRoutes != null && !spiRoutes.isUnsatisfied()) {
            routes = spiRoutes.stream().toList();
        }
        Map<String, Map<String, String>> configEndpoints = loadConfigEndpoints();
        resolver.initialize(routes, configEndpoints, defaultTimeoutSeconds, endpointRegistry);
        return resolver;
    }

    @Produces @ApplicationScoped @WorkerBackend @Priority(10)
    HttpWorkerExecutionManager executionManager(HttpEndpointResolver httpEndpointResolver,
                                                 WorkerFaultPublisher faultPublisher,
                                                 AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry,
                                                 WorkflowCompletionPublisher completionPublisher,
                                                 ObjectMapper objectMapper) {
        return new HttpWorkerExecutionManager(
            httpEndpointResolver, faultPublisher, asyncWorkerCompletionRegistry,
            completionPublisher, objectMapper, asyncTimeoutMinutes);
    }

    @Produces @ApplicationScoped
    HttpWorkerRuntime runtime(HttpEndpointResolver resolver) {
        return new HttpWorkerRuntime(resolver);
    }

    private Map<String, Map<String, String>> loadConfigEndpoints() {
        if (config == null) {
            return Map.of();
        }
        String prefix = "casehub.workers.http.endpoints.";
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String tag = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        result.computeIfAbsent(tag, k -> new LinkedHashMap<>()).put(prop, value)
                    );
                }
            }
        }
        return result;
    }
}
