package io.casehub.workers.http;

import java.util.Map;

public record ResolvedEndpoint(
    String url, String method, ExchangeMode mode,
    Map<String, String> headers, int timeoutSeconds
) {}
