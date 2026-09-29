package io.casehub.workers.githubactions;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;

import java.util.Set;
import java.util.logging.Logger;

public class GitHubActionsWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(GitHubActionsWorkerRuntime.class.getName());

    private final GitHubActionsTokenResolver tokenResolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    public GitHubActionsWorkerRuntime(GitHubActionsTokenResolver tokenResolver) {
        this.tokenResolver = tokenResolver;
    }

    @Override
    public String workerType() {
        return GitHubActionsWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public void initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) { return; }
        if (tokenResolver.hasToken()) {
            status = WorkerRuntimeStatus.RUNNING;
        } else {
            LOG.warning("GitHub Actions worker has no configured token — status FAULTED");
            status = WorkerRuntimeStatus.FAULTED;
        }
    }

    @Override
    public void shutdown() {
        status = WorkerRuntimeStatus.STOPPED;
    }

    @Override
    public Set<String> capabilities() {
        if (status != WorkerRuntimeStatus.RUNNING) {
            return Set.of();
        }
        return Set.of(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH,
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH
        );
    }
}
