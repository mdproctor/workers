package io.casehub.workers.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.http.HttpEndpointResolver;
import io.casehub.workers.http.HttpWorkerExecutionManager;
import io.casehub.workers.http.HttpWorkerRuntime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Map;

@AutoConfiguration
@ConditionalOnClass(HttpEndpointResolver.class)
public class HttpWorkerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public HttpEndpointResolver httpEndpointResolver(
            EndpointRegistry endpointRegistry,
            @Value("${casehub.workers.http.default-timeout-seconds:30}") int defaultTimeoutSeconds) {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        resolver.initialize(List.of(), Map.of(), defaultTimeoutSeconds, endpointRegistry);
        return resolver;
    }

    @Bean
    @ConditionalOnMissingBean
    public HttpWorkerExecutionManager httpWorkerExecutionManager(
            HttpEndpointResolver resolver,
            WorkerFaultPublisher faultPublisher,
            AsyncWorkerCompletionRegistry completionRegistry,
            WorkflowCompletionPublisher completionPublisher,
            ObjectMapper objectMapper,
            @Value("${casehub.workers.async.timeout-minutes:60}") int asyncTimeoutMinutes) {
        return new HttpWorkerExecutionManager(resolver, faultPublisher, completionRegistry,
            completionPublisher, objectMapper, asyncTimeoutMinutes);
    }

    @Bean
    @ConditionalOnMissingBean
    public HttpWorkerRuntime httpWorkerRuntime(HttpEndpointResolver resolver) {
        return new HttpWorkerRuntime(resolver);
    }
}
