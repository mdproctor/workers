package io.casehub.workers.mcp;

public interface McpSessionProvider {
    McpSession getSession(String serverName);
    void invalidate(String serverName);
}
