package io.casehub.workers.common;

public class PermanentFaultException extends RuntimeException {
    private final int statusCode;
    public PermanentFaultException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }
    public int statusCode() { return statusCode; }
}
