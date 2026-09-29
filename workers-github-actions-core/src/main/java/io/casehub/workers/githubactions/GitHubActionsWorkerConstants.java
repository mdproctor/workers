package io.casehub.workers.githubactions;

public final class GitHubActionsWorkerConstants {
    public static final String WORKER_TYPE = "github-actions";
    public static final String CAPABILITY_WORKFLOW_DISPATCH = "github-actions:workflow-dispatch";
    public static final String CAPABILITY_REPOSITORY_DISPATCH = "github-actions:repository-dispatch";
    private GitHubActionsWorkerConstants() {}
}
