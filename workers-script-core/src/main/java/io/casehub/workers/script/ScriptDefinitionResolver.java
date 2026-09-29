package io.casehub.workers.script;

import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ScriptDefinitionResolver implements WorkerCapabilityResolver<ScriptDefinition> {

    private static final String TAG_PREFIX = "script:";

    private final int defaultTimeoutSeconds;
    private final long defaultMaxOutputBytes;

    private volatile Map<String, ScriptDefinition> definitions = Map.of();

    public ScriptDefinitionResolver(int defaultTimeoutSeconds, long defaultMaxOutputBytes) {
        this.defaultTimeoutSeconds = defaultTimeoutSeconds;
        this.defaultMaxOutputBytes = defaultMaxOutputBytes;
    }

    public void initialize(Map<String, ScriptDefinition> scriptDefinitions) {
        definitions = Map.copyOf(scriptDefinitions);
    }

    @Override
    public ScriptDefinition resolve(String capabilityTag, String tenancyId) {
        if (!capabilityTag.startsWith(TAG_PREFIX)) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        String name = capabilityTag.substring(TAG_PREFIX.length());
        ScriptDefinition def = definitions.get(name);
        if (def == null) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        return def;
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
        return capabilities.stream()
            .filter(cap -> {
                if (!cap.startsWith(TAG_PREFIX)) return false;
                return definitions.containsKey(cap.substring(TAG_PREFIX.length()));
            })
            .findFirst();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(definitions.keySet().stream()
            .map(name -> TAG_PREFIX + name)
            .toList());
    }

    public ScriptDefinition buildFromConfig(String name, Map<String, String> props) {
        String command = props.get("command");
        if (command == null || command.isBlank()) {
            throw new WorkerProvisioningException(
                "Script '" + name + "' has no 'command' configured");
        }
        List<String> args = parseArgs(props.get("args"));
        String workingDirectory = props.get("working-directory");
        Map<String, String> env = extractEnvironment(props);
        int timeout = parseIntOrDefault(props.get("timeout-seconds"), defaultTimeoutSeconds);
        long maxOutput = parseLongOrDefault(props.get("max-output-bytes"), defaultMaxOutputBytes);
        return new ScriptDefinition(name, command, args, workingDirectory, env, timeout, maxOutput);
    }

    private List<String> parseArgs(String value) {
        if (value == null || value.isBlank()) return List.of();
        return List.of(value.split(","));
    }

    private Map<String, String> extractEnvironment(Map<String, String> props) {
        Map<String, String> env = new LinkedHashMap<>();
        String prefix = "environment.";
        props.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                env.put(key.substring(prefix.length()), value);
            }
        });
        return env.isEmpty() ? Map.of() : Map.copyOf(env);
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
