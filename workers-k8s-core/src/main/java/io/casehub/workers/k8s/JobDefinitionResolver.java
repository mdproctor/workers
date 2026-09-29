package io.casehub.workers.k8s;

import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class JobDefinitionResolver implements WorkerCapabilityResolver<JobDefinition> {

    private final String defaultNamespace;
    private final int defaultTimeoutSeconds;
    private final int defaultTtlAfterFinished;
    private final int defaultBackoffLimit;
    private final String defaultCleanup;
    private final long defaultMaxOutputBytes;
    private final long defaultMaxInputBytes;

    private volatile Map<String, JobDefinition> definitions = Map.of();

    public JobDefinitionResolver(String defaultNamespace,
                                  int defaultTimeoutSeconds,
                                  int defaultTtlAfterFinished,
                                  int defaultBackoffLimit,
                                  String defaultCleanup,
                                  long defaultMaxOutputBytes,
                                  long defaultMaxInputBytes) {
        this.defaultNamespace = defaultNamespace;
        this.defaultTimeoutSeconds = defaultTimeoutSeconds;
        this.defaultTtlAfterFinished = defaultTtlAfterFinished;
        this.defaultBackoffLimit = defaultBackoffLimit;
        this.defaultCleanup = defaultCleanup;
        this.defaultMaxOutputBytes = defaultMaxOutputBytes;
        this.defaultMaxInputBytes = defaultMaxInputBytes;
    }

    public void initialize(Map<String, JobDefinition> jobDefinitions) {
        definitions = Map.copyOf(jobDefinitions);
    }

    public JobDefinition buildFromConfig(String name, Map<String, String> props) {
        String image = props.get("image");
        String template = props.get("template");
        if ((image == null || image.isBlank()) && (template == null || template.isBlank())) {
            throw new WorkerProvisioningException(
                "K8s job '" + name + "' must have either 'image' or 'template' configured");
        }
        String capTag = K8sWorkerConstants.TAG_PREFIX + name;
        if (capTag.length() > 63) {
            throw new WorkerProvisioningException(
                "Capability tag '" + capTag + "' exceeds K8s 63-char label value limit");
        }
        String namespace = propOrDefault(props, "namespace", defaultNamespace);
        int timeout = parseIntOrDefault(props.get("timeout-seconds"), defaultTimeoutSeconds);
        int ttl = Math.max(300, parseIntOrDefault(props.get("ttl-after-finished"), defaultTtlAfterFinished));
        int backoff = parseIntOrDefault(props.get("backoff-limit"), defaultBackoffLimit);
        long maxOutput = parseLongOrDefault(props.get("max-output-bytes"), defaultMaxOutputBytes);
        CleanupPolicy cleanup = parseCleanup(props.get("cleanup"), defaultCleanup);
        List<String> command = parseList(props.get("command"));
        List<String> args = parseList(props.get("args"));
        Map<String, String> env = extractPrefixed(props, "environment.");
        Map<String, String> labels = extractPrefixed(props, "labels.");

        return new JobDefinition(name, namespace, image, command, args, template,
            props.get("cpu-request"), props.get("cpu-limit"),
            props.get("memory-request"), props.get("memory-limit"),
            timeout, ttl, backoff, maxOutput,
            props.get("service-account"), labels, env, cleanup);
    }

    @Override
    public JobDefinition resolve(String capabilityTag, String tenancyId) {
        if (!capabilityTag.startsWith(K8sWorkerConstants.TAG_PREFIX)) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        String name = capabilityTag.substring(K8sWorkerConstants.TAG_PREFIX.length());
        JobDefinition def = definitions.get(name);
        if (def == null) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        return def;
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
        return capabilities.stream()
            .filter(cap -> {
                if (!cap.startsWith(K8sWorkerConstants.TAG_PREFIX)) return false;
                return definitions.containsKey(cap.substring(K8sWorkerConstants.TAG_PREFIX.length()));
            })
            .findFirst();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(definitions.keySet().stream()
            .map(name -> K8sWorkerConstants.TAG_PREFIX + name)
            .toList());
    }

    public Set<String> namespaces() {
        return definitions.values().stream()
            .map(JobDefinition::namespace)
            .collect(Collectors.toUnmodifiableSet());
    }

    public long maxInputBytes() {
        return defaultMaxInputBytes;
    }

    private static String propOrDefault(Map<String, String> props, String key, String defaultValue) {
        String v = props.get(key);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    private static List<String> parseList(String value) {
        if (value == null || value.isBlank()) return List.of();
        return List.of(value.split(","));
    }

    private static Map<String, String> extractPrefixed(Map<String, String> props, String prefix) {
        Map<String, String> result = new LinkedHashMap<>();
        props.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), value);
            }
        });
        return result.isEmpty() ? Map.of() : Map.copyOf(result);
    }

    private static CleanupPolicy parseCleanup(String value, String defaultValue) {
        String v = (value != null && !value.isBlank()) ? value : defaultValue;
        return CleanupPolicy.valueOf(v.toUpperCase());
    }

    private static int parseIntOrDefault(String value, int defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try { return Integer.parseInt(value); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    private static long parseLongOrDefault(String value, long defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try { return Long.parseLong(value); }
        catch (NumberFormatException e) { return defaultValue; }
    }
}
