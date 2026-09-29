package io.casehub.workers.common;

import java.util.Map;

public record WorkerCompletionPayload(
    Map<String, Object> output,
    boolean faulted,
    String errorMessage
) {}
