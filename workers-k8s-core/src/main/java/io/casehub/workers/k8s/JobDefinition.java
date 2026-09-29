package io.casehub.workers.k8s;

import java.util.List;
import java.util.Map;

public record JobDefinition(
    String name,
    String namespace,
    String image,
    List<String> command,
    List<String> args,
    String template,
    String cpuRequest,
    String cpuLimit,
    String memoryRequest,
    String memoryLimit,
    int timeoutSeconds,
    int ttlAfterFinished,
    int backoffLimit,
    long maxOutputBytes,
    String serviceAccount,
    Map<String, String> labels,
    Map<String, String> environment,
    CleanupPolicy cleanup
) {}
