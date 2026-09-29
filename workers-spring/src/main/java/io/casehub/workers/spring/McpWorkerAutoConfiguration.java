package io.casehub.workers.spring;

import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.mcp.McpServerResolver;
import io.casehub.workers.mcp.McpSessionProvider;
import io.casehub.workers.mcp.McpWorkerExecutionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(McpServerResolver.class)
public class McpWorkerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public McpServerResolver mcpServerResolver(
            EndpointRegistry endpointRegistry,
            @Value("${casehub.workers.mcp.default-timeout-seconds:30}") int defaultTimeoutSeconds) {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(), defaultTimeoutSeconds, endpointRegistry);
        return resolver;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(McpSessionProvider.class)
    public McpWorkerExecutionManager mcpWorkerExecutionManager(
            McpServerResolver serverResolver,
            McpSessionProvider sessionProvider,
            WorkerFaultPublisher faultPublisher,
            WorkflowCompletionPublisher completionPublisher) {
        return new McpWorkerExecutionManager(serverResolver, sessionProvider, faultPublisher, completionPublisher);
    }
}
