package io.casehub.workers.spring.integration;

import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerFaultHandler;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerLifecycleOrchestrator;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.githubactions.GitHubActionsTokenResolver;
import io.casehub.workers.githubactions.GitHubActionsWorkerExecutionManager;
import io.casehub.workers.githubactions.GitHubActionsWorkerRuntime;
import io.casehub.workers.http.HttpEndpointResolver;
import io.casehub.workers.http.HttpWorkerExecutionManager;
import io.casehub.workers.http.HttpWorkerRuntime;
import io.casehub.workers.k8s.JobDefinitionResolver;
import io.casehub.workers.mcp.McpServerResolver;
import io.casehub.workers.scenario.ScenarioEndpointResolver;
import io.casehub.workers.scenario.ScenarioWorkerRuntime;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestStubConfiguration.class)
class SpringBootCompositionTest {

    @Autowired
    private ApplicationContext context;

    @LocalServerPort
    private int port;

    @Test
    void contextLoads() {
    }

    @Test
    void commonBeansRegistered() {
        assertThat(context.getBean(WorkerRetrySupport.class)).isNotNull();
        assertThat(context.getBean(WorkerFaultHandler.class)).isNotNull();
        assertThat(context.getBean(WorkerFaultPublisher.class)).isNotNull();
        assertThat(context.getBean(WorkflowCompletionPublisher.class)).isNotNull();
        assertThat(context.getBean(AsyncWorkerCompletionRegistry.class)).isNotNull();
        assertThat(context.getBean(WorkerLifecycleOrchestrator.class)).isNotNull();
    }

    @Test
    void httpWorkerBeansRegistered() {
        assertThat(context.getBean(HttpEndpointResolver.class)).isNotNull();
        assertThat(context.getBean(HttpWorkerExecutionManager.class)).isNotNull();
        assertThat(context.getBean(HttpWorkerRuntime.class)).isNotNull();
    }

    @Test
    void gitHubActionsBeansRegistered() {
        assertThat(context.getBean(GitHubActionsTokenResolver.class)).isNotNull();
        assertThat(context.getBean(GitHubActionsWorkerExecutionManager.class)).isNotNull();
        assertThat(context.getBean(GitHubActionsWorkerRuntime.class)).isNotNull();
    }

    @Test
    void mcpResolverRegistered() {
        assertThat(context.getBean(McpServerResolver.class)).isNotNull();
    }

    @Test
    void scenarioBeansRegistered() {
        assertThat(context.getBean(ScenarioEndpointResolver.class)).isNotNull();
        assertThat(context.getBean(ScenarioWorkerRuntime.class)).isNotNull();
    }

    @Test
    void k8sResolverRegistered() {
        assertThat(context.getBean(JobDefinitionResolver.class)).isNotNull();
    }

    @Test
    void healthCheckReturnsUp() throws Exception {
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/actuator/health"))
                .GET()
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("UP");
    }
}
