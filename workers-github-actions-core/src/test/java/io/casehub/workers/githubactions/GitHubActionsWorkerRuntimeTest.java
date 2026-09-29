package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.casehub.workers.common.WorkerRuntimeStatus;
import org.junit.jupiter.api.Test;

class GitHubActionsWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        var tokenResolver = mock(GitHubActionsTokenResolver.class);
        var runtime = new GitHubActionsWorkerRuntime(tokenResolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("github-actions");
    }

    @Test
    void initialize_withToken_transitionsToRunning() {
        var tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(true);

        var runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_withoutToken_transitionsToFaulted() {
        var tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(false);

        var runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initialize_faultedThenTokenAdded_recoversToRunning() {
        var tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(false).thenReturn(true);

        var runtime = new GitHubActionsWorkerRuntime(tokenResolver);

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);

        runtime.initialize();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void capabilities_returnsStaticSet() {
        var tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(true);

        var runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize();

        assertThat(runtime.capabilities())
            .containsExactlyInAnyOrder(
                "github-actions:workflow-dispatch",
                "github-actions:repository-dispatch"
            );
    }

    @Test
    void capabilities_beforeInit_isEmpty() {
        var tokenResolver = mock(GitHubActionsTokenResolver.class);
        var runtime = new GitHubActionsWorkerRuntime(tokenResolver);

        assertThat(runtime.capabilities()).isEmpty();
    }

    @Test
    void shutdown_transitionsToStopped() {
        var tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(true);

        var runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize();
        runtime.shutdown();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }
}
