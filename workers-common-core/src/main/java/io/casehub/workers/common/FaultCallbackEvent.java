package io.casehub.workers.common;

public record FaultCallbackEvent(PendingCompletion pending, Throwable cause) {}
