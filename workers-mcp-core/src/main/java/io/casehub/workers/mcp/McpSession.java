package io.casehub.workers.mcp;

import java.util.concurrent.atomic.AtomicLong;

public class McpSession {
    private final String sessionId;
    private final String protocolVersion;
    private final AtomicLong requestIdCounter;

    public McpSession(String sessionId, String protocolVersion) {
        this.sessionId = sessionId;
        this.protocolVersion = protocolVersion;
        this.requestIdCounter = new AtomicLong(2);
    }

    public String sessionId() { return sessionId; }
    public String protocolVersion() { return protocolVersion; }
    public long nextRequestId() { return requestIdCounter.getAndIncrement(); }
    public boolean hasSessionId() { return sessionId != null && !sessionId.isBlank(); }
}
