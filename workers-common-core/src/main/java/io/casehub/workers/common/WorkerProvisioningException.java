package io.casehub.workers.common;

public class WorkerProvisioningException extends RuntimeException {
    public WorkerProvisioningException(String message) {
        super(message);
    }
    public WorkerProvisioningException(String message, Throwable cause) {
        super(message, cause);
    }
    public static WorkerProvisioningException noRouteFound(String capabilities) {
        return new WorkerProvisioningException("No route found for capabilities: " + capabilities);
    }
}
