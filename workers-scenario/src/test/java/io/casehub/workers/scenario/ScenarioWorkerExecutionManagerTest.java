package io.casehub.workers.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.casehub.worker.api.Capability;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.testing.WorkerTestSupport;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ScenarioWorkerExecutionManagerTest {

    ScenarioWorkerExecutionManager manager;
    ScenarioEndpointResolver resolver;
    AsyncWorkerCompletionRegistry completionRegistry;
    WorkerFaultPublisher faultPublisher;
    WebClient webClient;

    @BeforeEach
    void setUp() {
        manager = new ScenarioWorkerExecutionManager();
        resolver = new ScenarioEndpointResolver();
        completionRegistry = mock(AsyncWorkerCompletionRegistry.class);
        faultPublisher = mock(WorkerFaultPublisher.class);
        webClient = mock(WebClient.class);

        manager.endpointResolver = resolver;
        manager.completionRegistry = completionRegistry;
        manager.faultPublisher = faultPublisher;
        manager.webClient = webClient;
        manager.callbackBaseUrl = "https://engine.example.com";
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
            eq(ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT),
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_rawYaml_registersAndDispatches() {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("raw", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);

        @SuppressWarnings("unchecked")
        HttpRequest<Buffer> request = mock(HttpRequest.class, RETURNS_DEEP_STUBS);
        HttpResponse<Buffer> response = mockResponse(200, "OK");
        when(request.putHeader(any(String.class), any(String.class))).thenReturn(request);
        when(request.timeout(anyLong())).thenReturn(request);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        when(webClient.requestAbs(eq(HttpMethod.POST), eq("https://pages.example.com/graphql")))
            .thenReturn(request);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:raw"),
            WorkerTestSupport.testCapability("scenario:raw"),
            Map.of("yaml", "scenario: test\nsteps:\n  - navigate: /home"));

        verify(completionRegistry).register(
            eq("scenario"), eq(ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT),
            any(), any(), eq(1L), any(Duration.class), any());
        verify(request).sendJson(any());
    }

    @Test
    void submit_namedScript_fetchesYamlThenDispatches() {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);

        // Library fetch (GET)
        @SuppressWarnings("unchecked")
        HttpRequest<Buffer> libraryRequest = mock(HttpRequest.class, RETURNS_DEEP_STUBS);
        HttpResponse<Buffer> libraryResponse = mockResponse(200, "OK");
        when(libraryResponse.bodyAsString()).thenReturn("scenario: onboard\nsteps:\n  - navigate: /onboard");
        when(libraryRequest.putHeader(any(String.class), any(String.class))).thenReturn(libraryRequest);
        when(libraryRequest.timeout(anyLong())).thenReturn(libraryRequest);
        when(libraryRequest.send()).thenReturn(Uni.createFrom().item(libraryResponse));

        when(webClient.requestAbs(eq(HttpMethod.GET), eq("https://pages.example.com/scenario/library/onboard/yaml")))
            .thenReturn(libraryRequest);

        // GraphQL submit (POST)
        @SuppressWarnings("unchecked")
        HttpRequest<Buffer> submitRequest = mock(HttpRequest.class, RETURNS_DEEP_STUBS);
        HttpResponse<Buffer> submitResponse = mockResponse(200, "OK");
        when(submitRequest.putHeader(any(String.class), any(String.class))).thenReturn(submitRequest);
        when(submitRequest.timeout(anyLong())).thenReturn(submitRequest);
        when(submitRequest.sendJson(any())).thenReturn(Uni.createFrom().item(submitResponse));

        when(webClient.requestAbs(eq(HttpMethod.POST), eq("https://pages.example.com/graphql")))
            .thenReturn(submitRequest);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:onboard"),
            WorkerTestSupport.testCapability("scenario:onboard"),
            Map.of("scriptName", "onboard"));

        verify(webClient).requestAbs(HttpMethod.GET, "https://pages.example.com/scenario/library/onboard/yaml");
        verify(submitRequest).sendJson(any());
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
            eq(ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT),
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_scriptNotFound404_permanentFault() {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("missing-script", "https://pages.example.com/graphql", 600)
        ), 300);

        @SuppressWarnings("unchecked")
        HttpRequest<Buffer> libraryRequest = mock(HttpRequest.class, RETURNS_DEEP_STUBS);
        HttpResponse<Buffer> libraryResponse = mockResponse(404, "Not Found");
        when(libraryRequest.putHeader(any(String.class), any(String.class))).thenReturn(libraryRequest);
        when(libraryRequest.timeout(anyLong())).thenReturn(libraryRequest);
        when(libraryRequest.send()).thenReturn(Uni.createFrom().item(libraryResponse));

        when(webClient.requestAbs(eq(HttpMethod.GET), eq("https://pages.example.com/scenario/library/missing-script/yaml")))
            .thenReturn(libraryRequest);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:missing-script"),
            WorkerTestSupport.testCapability("scenario:missing-script"),
            Map.of("scriptName", "missing-script"));

        verify(faultPublisher).fault(
            eq(ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT),
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_5arg_passesNullBindingName() {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);

        @SuppressWarnings("unchecked")
        HttpRequest<Buffer> request = mock(HttpRequest.class, RETURNS_DEEP_STUBS);
        HttpResponse<Buffer> response = mockResponse(200, "OK");
        when(request.putHeader(any(String.class), any(String.class))).thenReturn(request);
        when(request.timeout(anyLong())).thenReturn(request);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        when(webClient.requestAbs(eq(HttpMethod.POST), eq("https://pages.example.com/graphql")))
            .thenReturn(request);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:onboard"),
            WorkerTestSupport.testCapability("scenario:onboard"),
            Map.of("yaml", "scenario: test"));

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionRegistry).register(any(), any(), ctxCaptor.capture(), any(), any(), any(Duration.class), any());
        assertThat(ctxCaptor.getValue().bindingName()).isNull();
    }

    @Test
    void submit_6arg_passesBindingName() {
        resolver.initialize(List.of(
            new ScenarioEndpointResolver.EndpointConfig("onboard", "https://pages.example.com/graphql", 600)
        ), 300);

        PendingCompletion pending = stubPendingCompletion();
        when(completionRegistry.register(any(), any(), any(), any(), any(), any(Duration.class), any()))
            .thenReturn(pending);

        @SuppressWarnings("unchecked")
        HttpRequest<Buffer> request = mock(HttpRequest.class, RETURNS_DEEP_STUBS);
        HttpResponse<Buffer> response = mockResponse(200, "OK");
        when(request.putHeader(any(String.class), any(String.class))).thenReturn(request);
        when(request.timeout(anyLong())).thenReturn(request);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        when(webClient.requestAbs(eq(HttpMethod.POST), eq("https://pages.example.com/graphql")))
            .thenReturn(request);

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:onboard"),
            WorkerTestSupport.testCapability("scenario:onboard"),
            Map.of("yaml", "scenario: test"), "binding-x");

        ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
            ArgumentCaptor.forClass(WorkerCorrelationContext.class);
        verify(completionRegistry).register(any(), any(), ctxCaptor.capture(), any(), any(), any(Duration.class), any());
        assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
    }

    // --- helpers ---

    @SuppressWarnings("unchecked")
    private HttpResponse<Buffer> mockResponse(int statusCode, String statusMessage) {
        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(statusCode);
        when(response.statusMessage()).thenReturn(statusMessage);
        when(response.bodyAsString()).thenReturn("{}");
        when(response.getHeader("Retry-After")).thenReturn(null);
        return response;
    }

    private PendingCompletion stubPendingCompletion() {
        return new PendingCompletion(
            UUID.randomUUID().toString(), "scenario",
            ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT,
            null, UUID.randomUUID().toString(), null, 1L,
            Instant.now(), Instant.now().plusSeconds(600), Map.of());
    }
}
