package io.casehub.workers.spring;

import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.githubactions.GitHubActionsTokenResolver;
import io.casehub.workers.githubactions.GitHubActionsWorkerExecutionManager;
import io.casehub.workers.githubactions.GitHubActionsWorkerRuntime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.Map;
import java.util.Optional;

@AutoConfiguration
@ConditionalOnClass(GitHubActionsTokenResolver.class)
public class GitHubActionsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public GitHubActionsTokenResolver gitHubActionsTokenResolver(
            @Value("${casehub.workers.github-actions.token:}") String globalToken,
            @Value("#{${casehub.workers.github-actions.tokens:{}}}") Map<String, String> orgTokens,
            @Value("${casehub.workers.github-actions.api-base-url:https://api.github.com}") String apiBaseUrl) {
        Optional<String> token = globalToken.isBlank() ? Optional.empty() : Optional.of(globalToken);
        return new GitHubActionsTokenResolver(token, orgTokens, apiBaseUrl);
    }

    @Bean
    @ConditionalOnMissingBean
    public GitHubActionsWorkerExecutionManager gitHubActionsWorkerExecutionManager(
            GitHubActionsTokenResolver tokenResolver,
            WorkerFaultPublisher faultPublisher,
            WorkflowCompletionPublisher completionPublisher) {
        return new GitHubActionsWorkerExecutionManager(tokenResolver, faultPublisher, completionPublisher);
    }

    @Bean
    @ConditionalOnMissingBean
    public GitHubActionsWorkerRuntime gitHubActionsWorkerRuntime(
            GitHubActionsTokenResolver tokenResolver) {
        return new GitHubActionsWorkerRuntime(tokenResolver);
    }
}
