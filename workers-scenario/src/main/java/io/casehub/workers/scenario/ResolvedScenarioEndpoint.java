package io.casehub.workers.scenario;

public record ResolvedScenarioEndpoint(
    String name,
    String url,
    int timeoutSeconds
) {}
