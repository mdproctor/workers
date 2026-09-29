package io.casehub.workers.script;

import io.casehub.engine.common.spi.scheduler.WorkerBackend;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.Map;

@ApplicationScoped
public class ScriptWorkerBeans {

    @Inject Config config;

    @ConfigProperty(name = "casehub.workers.script.default-timeout-seconds", defaultValue = "300")
    int defaultTimeoutSeconds;

    @ConfigProperty(name = "casehub.workers.script.max-output-bytes", defaultValue = "1048576")
    long defaultMaxOutputBytes;

    private ScriptWorkerExecutionManager executionManager;

    @Produces @ApplicationScoped
    ScriptDefinitionResolver resolver() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(
            defaultTimeoutSeconds, defaultMaxOutputBytes);
        Map<String, Map<String, String>> configScripts = loadConfigScripts();
        Map<String, ScriptDefinition> parsed = new LinkedHashMap<>();
        configScripts.forEach((name, props) -> parsed.put(name, resolver.buildFromConfig(name, props)));
        resolver.initialize(parsed);
        return resolver;
    }

    @Produces @ApplicationScoped @WorkerBackend @Priority(10)
    ScriptWorkerExecutionManager executionManager(ScriptDefinitionResolver resolver,
                                                   WorkerFaultPublisher faultPublisher,
                                                   WorkflowCompletionPublisher completionPublisher) {
        executionManager = new ScriptWorkerExecutionManager(resolver, faultPublisher, completionPublisher);
        return executionManager;
    }

    @Produces @ApplicationScoped
    ScriptWorkerRuntime runtime(ScriptDefinitionResolver resolver) {
        return new ScriptWorkerRuntime(resolver);
    }

    @PreDestroy
    void shutdown() {
        if (executionManager != null) {
            executionManager.shutdown();
        }
    }

    private Map<String, Map<String, String>> loadConfigScripts() {
        if (config == null) return Map.of();
        String prefix = "casehub.workers.script.scripts.";
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String name = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        result.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(prop, value)
                    );
                }
            }
        }
        return result;
    }
}
