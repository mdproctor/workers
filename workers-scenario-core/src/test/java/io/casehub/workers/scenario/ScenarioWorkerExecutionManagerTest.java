package io.casehub.workers.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.casehub.worker.api.Capability;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.testing.WorkerTestSupport;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class ScenarioWorkerExecutionManagerTest {

    ScenarioWorkerExecutionManager manager;
    ScenarioEndpointResolver resolver;
    AsyncWorkerCompletionRegistry completionRegistry;
    WorkerFaultPublisher faultPublisher;
    HttpClient httpClient;

    @BeforeEach
    void setUp() {
        resolver = new ScenarioEndpointResolver();
        completionRegistry = mock(AsyncWorkerCompletionRegistry.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        httpClient = mock(HttpClient.class);

        manager = new ScenarioWorkerExecutionManager(resolver, completionRegistry, faultPublisher, "https://engine.example.com");
        manager.httpClient = httpClient;
    }

    @Test
    void supports_delegatesToResolver() {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        assertThat(manager.supports("scenario:onboard", "t1")).isTrue();
        assertThat(manager.supports("scenario:unknown", "t1")).isFalse();
    }

    @Test
    void getActiveWorkCount_returnsRegistryCount() {
        when(completionRegistry.countByWorkerName("w1")).thenReturn(3);
        assertThat(manager.getActiveWorkCount("w1")).isEqualTo(3);
    }

    @Test
    void submit_unknownCapability_permanentFault() {
        resolver.initialize(List.of(), 300);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:missing"),
            WorkerTestSupport.testCapability("scenario:missing"),
            Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_rawYaml_registersAndDispatches() throws Exception {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("raw", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);
        stubResponse(200, null);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:raw"),
            WorkerTestSupport.testCapability("scenario:raw"),
            Map.of("yaml", "scenario: test\nsteps:\n  - navigate: /home"));

        verify(completionRegistry).register(
            eq("scenario"),
            any(), any(), eq(1L), any(Duration.class), any());
        verify(httpClient).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    void submit_namedScript_fetchesYamlThenDispatches() throws Exception {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);

        HttpResponse<String> libraryResponse = mockResponse(200, "scenario: onboard\nsteps:\n  - navigate: /onboard");
        HttpResponse<String> submitResponse = mockResponse(200, "{}");

        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenAnswer(invocation -> {
                HttpRequest req = invocation.getArgument(0);
                if (req.uri().toString().contains("/library/")) {
                    return libraryResponse;
                }
                return submitResponse;
            });

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:onboard"),
            WorkerTestSupport.testCapability("scenario:onboard"),
            Map.of("scriptName", "onboard"));

        ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient, org.mockito.Mockito.times(2)).send(captor.capture(), any(HttpResponse.BodyHandler.class));
        List<HttpRequest> requests = captor.getAllValues();
        assertThat(requests.get(0).uri().toString())
            .isEqualTo("https://pages.example.com/scenario/library/onboard/yaml");
        assertThat(requests.get(0).method()).isEqualTo("GET");
        assertThat(requests.get(1).uri().toString())
            .isEqualTo("https://pages.example.com/graphql");
        assertThat(requests.get(1).method()).isEqualTo("POST");
    }

    @Test
    void submit_noYamlAndNoScriptName_permanentFault() {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("empty", "https://pages.example.com/graphql", 600)
        ), 300);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:empty"),
            WorkerTestSupport.testCapability("scenario:empty"),
            Map.of());

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_scriptNotFound404_permanentFault() throws Exception {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("missing-script", "https://pages.example.com/graphql", 600)
        ), 300);

        stubResponse(404, null);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:missing-script"),
            WorkerTestSupport.testCapability("scenario:missing-script"),
            Map.of("scriptName", "missing-script"));

        verify(faultPublisher).fault(
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_5arg_passesNullBindingName() throws Exception {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);
        stubResponse(200, null);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:onboard"),
            WorkerTestSupport.testCapability("scenario:onboard"),
            Map.of("yaml", "scenario: test"));

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionRegistry).register(any(), ctxCaptor.capture(), any(), any(), any(Duration.class), any());
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    @Test
    void submit_6arg_passesBindingName() throws Exception {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);
        stubResponse(200, null);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:onboard"),
            WorkerTestSupport.testCapability("scenario:onboard"),
            Map.of("yaml", "scenario: test"), "binding-x");

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionRegistry).register(any(), ctxCaptor.capture(), any(), any(), any(Duration.class), any());
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    private void stubResponse(int status, String retryAfter) throws Exception {
        HttpResponse<String> response = mockResponse(status, "{}");
        Map<String, List<String>> headerMap = new HashMap<>();
        if (retryAfter != null) {
            headerMap.put("Retry-After", List.of(retryAfter));
        }
        when(response.headers()).thenReturn(HttpHeaders.of(headerMap, (a, b) -> true));
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(response);
    }

    private HttpResponse<String> mockResponse(int statusCode, String body) {
        HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        when(response.body()).thenReturn(body);
        when(response.headers()).thenReturn(HttpHeaders.of(Map.of(), (a, b) -> true));
        return response;
    }

    private PendingCompletion stubPendingCompletion() {
        return new PendingCompletion(
            UUID.randomUUID().toString(), "scenario",
            null, UUID.randomUUID().toString(), null, 1L,
            Instant.now(), Instant.now().plusSeconds(600), Map.of());
    }
}
