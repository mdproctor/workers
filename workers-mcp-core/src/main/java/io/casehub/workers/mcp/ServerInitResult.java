package io.casehub.workers.mcp;

import java.util.Set;

public record ServerInitResult(String serverName, boolean success, McpSession session,
                               Set<String> discoveredTools, Throwable error) {

    public static ServerInitResult success(String serverName, McpSession session, Set<String> discoveredTools) {
        return new ServerInitResult(serverName, true, session, discoveredTools, null);
    }

    public static ServerInitResult failure(String serverName, Throwable error) {
        return new ServerInitResult(serverName, false, null, Set.of(), error);
    }
}
