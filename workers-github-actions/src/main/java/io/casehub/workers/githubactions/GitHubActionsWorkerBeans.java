package io.casehub.workers.githubactions;

import io.casehub.engine.common.spi.scheduler.WorkerBackend;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class GitHubActionsWorkerBeans {

    @ConfigProperty(name = "casehub.workers.github-actions.token")
    Optional<String> globalToken;

    @ConfigProperty(name = "casehub.workers.github-actions.tokens", defaultValue = "")
    Map<String, String> orgTokens;

    @ConfigProperty(name = "casehub.workers.github-actions.api-base-url",
                    defaultValue = "https://api.github.com")
    String apiBaseUrl;

    @Produces @ApplicationScoped
    GitHubActionsTokenResolver tokenResolver() {
        return new GitHubActionsTokenResolver(globalToken, orgTokens, apiBaseUrl);
    }

    @Produces @ApplicationScoped @WorkerBackend @Priority(10)
    GitHubActionsWorkerExecutionManager executionManager(GitHubActionsTokenResolver tokenResolver,
                                                         WorkerFaultPublisher faultPublisher,
                                                         WorkflowCompletionPublisher completionPublisher) {
        return new GitHubActionsWorkerExecutionManager(tokenResolver, faultPublisher, completionPublisher);
    }

    @Produces @ApplicationScoped
    GitHubActionsWorkerRuntime runtime(GitHubActionsTokenResolver tokenResolver) {
        return new GitHubActionsWorkerRuntime(tokenResolver);
    }
}
