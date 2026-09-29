package io.casehub.workers.k8s;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.Map;

@ApplicationScoped
public class K8sWorkerBeans {

    @Inject Config config;

    @ConfigProperty(name = "casehub.workers.k8s.namespace", defaultValue = "default")
    String defaultNamespace;

    @ConfigProperty(name = "casehub.workers.k8s.timeout-seconds", defaultValue = "3600")
    int defaultTimeoutSeconds;

    @ConfigProperty(name = "casehub.workers.k8s.ttl-after-finished", defaultValue = "600")
    int defaultTtlAfterFinished;

    @ConfigProperty(name = "casehub.workers.k8s.backoff-limit", defaultValue = "0")
    int defaultBackoffLimit;

    @ConfigProperty(name = "casehub.workers.k8s.cleanup", defaultValue = "delete")
    String defaultCleanup;

    @ConfigProperty(name = "casehub.workers.k8s.max-output-bytes", defaultValue = "1048576")
    long defaultMaxOutputBytes;

    @ConfigProperty(name = "casehub.workers.k8s.max-input-bytes", defaultValue = "262144")
    long defaultMaxInputBytes;

    private JobDefinitionResolver resolver;

    @Produces @ApplicationScoped
    JobDefinitionResolver jobDefinitionResolver() {
        resolver = new JobDefinitionResolver(
            defaultNamespace, defaultTimeoutSeconds, defaultTtlAfterFinished,
            defaultBackoffLimit, defaultCleanup, defaultMaxOutputBytes, defaultMaxInputBytes);
        Map<String, Map<String, String>> configJobs = loadConfigJobs();
        Map<String, JobDefinition> parsed = new LinkedHashMap<>();
        configJobs.forEach((name, props) -> parsed.put(name, resolver.buildFromConfig(name, props)));
        resolver.initialize(parsed);
        return resolver;
    }

    private Map<String, Map<String, String>> loadConfigJobs() {
        if (config == null) return Map.of();
        String prefix = "casehub.workers.k8s.jobs.";
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String name = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        result.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(prop, value));
                }
            }
        }
        return result;
    }
}
