package io.casehub.workers.common;

import java.util.Set;

public final class WorkerProvisionerSupport {

    private WorkerProvisionerSupport() {}

    public static void validateCapabilities(Set<String> requested, Set<String> supported) {
        for (String cap : requested) {
            if (!supported.contains(cap)) {
                throw new WorkerProvisioningException(
                    "Unsupported capability: " + cap + ". Supported: " + supported);
            }
        }
    }

    public static WorkerProvisioningException wrap(Throwable t, String capability) {
        if (t instanceof WorkerProvisioningException wpe) return wpe;
        return new WorkerProvisioningException(
            "Provisioning failed for capability: " + capability, t);
    }
}
