package io.casehub.workers.common;

import java.util.Optional;
import java.util.Set;

public interface WorkerCapabilityResolver<T> {
    T resolve(String capabilityTag, String tenancyId);
    Optional<String> firstMatch(Set<String> capabilities, String tenancyId);
    Set<String> capabilities();

    default boolean canResolve(String capabilityTag, String tenancyId) {
        return capabilities().contains(capabilityTag);
    }
}
