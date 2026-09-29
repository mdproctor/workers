package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import io.casehub.worker.api.Capability;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PendingCompletionTest {
    @Test
    void recordComponents() {
        Capability cap = Capability.of("send-email", "", "");
        Instant now = Instant.now();
        Instant expires = now.plusSeconds(3600);
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "camel", null, "token-abc", cap, 42L, now, expires, Map.of("key", "val"));
        assertThat(pending.dispatchId()).isEqualTo("dispatch-1");
        assertThat(pending.workerType()).isEqualTo("camel");
        assertThat(pending.callbackToken()).isEqualTo("token-abc");
        assertThat(pending.capability()).isSameAs(cap);
        assertThat(pending.eventLogId()).isEqualTo(42L);
        assertThat(pending.registeredAt()).isEqualTo(now);
        assertThat(pending.expiresAt()).isEqualTo(expires);
        assertThat(pending.provisionerMeta()).containsEntry("key", "val");
    }
}
