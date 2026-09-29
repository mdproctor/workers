package io.casehub.workers.http;

import java.util.Map;

public interface HttpWorkerRoute {
    String capabilityTag();
    String url();
    default String method() { return "POST"; }
    default ExchangeMode exchangeMode() { return ExchangeMode.SYNC; }
    default Map<String, String> headers() { return Map.of(); }
    default int timeoutSeconds() { return -1; }
}
