package io.casehub.workers.mcp;

import java.util.Map;
import java.util.Set;

public record ResolvedMcpServer(
    String name,
    String url,
    int timeoutSeconds,
    Map<String, String> headers,
    Set<String> tools
) {}
