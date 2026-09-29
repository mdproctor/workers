package io.casehub.workers.spring;

import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.scenario.ScenarioEndpointResolver;
import io.casehub.workers.scenario.ScenarioWorkerExecutionManager;
import io.casehub.workers.scenario.ScenarioWorkerRuntime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.List;

@AutoConfiguration
@ConditionalOnClass(ScenarioEndpointResolver.class)
public class ScenarioWorkerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ScenarioEndpointResolver scenarioEndpointResolver(
            EndpointRegistry endpointRegistry,
            @Value("${casehub.workers.scenario.default-timeout-seconds:600}") int defaultTimeoutSeconds) {
        ScenarioEndpointResolver resolver = new ScenarioEndpointResolver();
        resolver.initialize(List.of(), defaultTimeoutSeconds, endpointRegistry);
        return resolver;
    }

    @Bean
    @ConditionalOnMissingBean
    public ScenarioWorkerExecutionManager scenarioWorkerExecutionManager(
            ScenarioEndpointResolver resolver,
            AsyncWorkerCompletionRegistry completionRegistry,
            WorkerFaultPublisher faultPublisher,
            @Value("${casehub.workers.callback-base-url:}") String callbackBaseUrl) {
        return new ScenarioWorkerExecutionManager(resolver, completionRegistry, faultPublisher, callbackBaseUrl);
    }

    @Bean
    @ConditionalOnMissingBean
    public ScenarioWorkerRuntime scenarioWorkerRuntime(ScenarioEndpointResolver resolver) {
        return new ScenarioWorkerRuntime(resolver);
    }
}
