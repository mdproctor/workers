package io.casehub.workers.spring;

import io.casehub.workers.k8s.JobDefinitionResolver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(JobDefinitionResolver.class)
public class K8sWorkerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public JobDefinitionResolver jobDefinitionResolver(
            @Value("${casehub.workers.k8s.namespace:default}") String defaultNamespace,
            @Value("${casehub.workers.k8s.timeout-seconds:3600}") int defaultTimeoutSeconds,
            @Value("${casehub.workers.k8s.ttl-after-finished:600}") int defaultTtlAfterFinished,
            @Value("${casehub.workers.k8s.backoff-limit:0}") int defaultBackoffLimit,
            @Value("${casehub.workers.k8s.cleanup:delete}") String defaultCleanup,
            @Value("${casehub.workers.k8s.max-output-bytes:1048576}") long defaultMaxOutputBytes,
            @Value("${casehub.workers.k8s.max-input-bytes:262144}") long defaultMaxInputBytes) {
        return new JobDefinitionResolver(defaultNamespace, defaultTimeoutSeconds,
            defaultTtlAfterFinished, defaultBackoffLimit, defaultCleanup,
            defaultMaxOutputBytes, defaultMaxInputBytes);
    }
}
