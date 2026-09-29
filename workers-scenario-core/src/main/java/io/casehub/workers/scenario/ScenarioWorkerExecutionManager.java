package io.casehub.workers.scenario;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkerRetrySupport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.logging.Logger;

public class ScenarioWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(ScenarioWorkerExecutionManager.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(30);

    private final ScenarioEndpointResolver endpointResolver;
    private final AsyncWorkerCompletionRegistry completionRegistry;
    private final WorkerFaultPublisher faultPublisher;
    private final String callbackBaseUrl;

    HttpClient httpClient = HttpClient.newHttpClient();

    public ScenarioWorkerExecutionManager(ScenarioEndpointResolver endpointResolver,
                                           AsyncWorkerCompletionRegistry completionRegistry,
                                           WorkerFaultPublisher faultPublisher,
                                           String callbackBaseUrl) {
        this.endpointResolver = endpointResolver;
        this.completionRegistry = completionRegistry;
        this.faultPublisher = faultPublisher;
        this.callbackBaseUrl = callbackBaseUrl;
    }

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData) {
        submit(eventLogId, instance, worker, capability, inputData, null);
    }

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData,
                       String bindingName) {
        ResolvedScenarioEndpoint endpoint;
        try {
            endpoint = endpointResolver.resolve(capability.name(), instance.tenancyId);
        } catch (WorkerProvisioningException e) {
            WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);
            faultPublisher.fault(
                ctx, capability, eventLogId,
                new PermanentFaultException(0, e.getMessage()));
            return;
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);

        try {
            String yaml = resolveYaml(inputData, endpoint);
            PendingCompletion pending = completionRegistry.register(
                ScenarioWorkerConstants.WORKER_TYPE,
                ctx, capability, eventLogId, DEFAULT_TTL, Map.of());

            dispatchGraphQL(endpoint, yaml, inputData, pending);
        } catch (Exception t) {
            faultPublisher.fault(
                ctx, capability, eventLogId, t);
        }
    }

    @Override
    public boolean supports(String capabilityName, String tenancyId) {
        return endpointResolver.canResolve(capabilityName, tenancyId);
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return completionRegistry.countByWorkerName(workerId);
    }

    private String resolveYaml(Map<String, Object> inputData, ResolvedScenarioEndpoint endpoint) {
        Object yaml = inputData.get("yaml");
        Object scriptName = inputData.get("scriptName");

        if (scriptName != null && !scriptName.toString().isBlank()) {
            return fetchYamlFromLibrary(endpoint, scriptName.toString());
        }

        if (yaml != null && !yaml.toString().isBlank()) {
            return yaml.toString();
        }

        throw new PermanentFaultException(0,
            "inputData must contain either 'yaml' or 'scriptName'");
    }

    private String fetchYamlFromLibrary(ResolvedScenarioEndpoint endpoint, String scriptName) {
        String baseUrl = endpoint.url().replaceAll("/graphql$", "");
        String libraryUrl = baseUrl + "/scenario/library/" + scriptName + "/yaml";

        HttpRequest request = HttpRequest.newBuilder(URI.create(libraryUrl))
            .GET()
            .header("Accept", "text/yaml")
            .timeout(Duration.ofSeconds(endpoint.timeoutSeconds()))
            .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to fetch scenario script: " + e.getMessage());
        }

        int status = response.statusCode();

        if (status >= 200 && status < 300) {
            return response.body();
        }
        if (status == 404) {
            throw new PermanentFaultException(404,
                "Scenario script not found: " + scriptName);
        }
        if (status == 429) {
            throw WorkerRetrySupport.parseRetryAfter(
                response.headers().firstValue("Retry-After").orElse(null), status, "Too Many Requests");
        }
        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status, status + " Client Error");
        }
        throw new RuntimeException(status + " Server Error");
    }

    @SuppressWarnings("unchecked")
    private void dispatchGraphQL(ResolvedScenarioEndpoint endpoint, String yaml,
                                  Map<String, Object> inputData, PendingCompletion pending) {
        String callbackUrl = callbackBaseUrl + "/workers/complete/" + pending.dispatchId();

        Map<String, Object> params = inputData.containsKey("params")
            ? (Map<String, Object>) inputData.get("params")
            : Map.of();
        boolean paused = inputData.containsKey("paused")
            && Boolean.TRUE.equals(inputData.get("paused"));

        String yamlJson;
        String paramsJson;
        try {
            yamlJson = OBJECT_MAPPER.writeValueAsString(yaml);
            paramsJson = OBJECT_MAPPER.writeValueAsString(params);
        } catch (Exception e) {
            throw new PermanentFaultException(0, "Failed to serialize input: " + e.getMessage());
        }

        String mutation = "mutation { scenarioSubmit(yaml: " + yamlJson
            + ", params: " + paramsJson
            + ", callbackUrl: \"" + callbackUrl + "\""
            + ", dispatchId: \"" + pending.dispatchId() + "\""
            + ", paused: " + paused
            + ") { scenario progress } }";

        ObjectNode body = OBJECT_MAPPER.createObjectNode().put("query", mutation);

        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint.url()))
            .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .timeout(Duration.ofSeconds(endpoint.timeoutSeconds()))
            .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to dispatch scenario: " + e.getMessage());
        }

        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            LOG.fine("Scenario dispatched: " + pending.dispatchId() + " → " + endpoint.url());
            return;
        }
        if (status == 429) {
            throw WorkerRetrySupport.parseRetryAfter(
                response.headers().firstValue("Retry-After").orElse(null), status, "Too Many Requests");
        }
        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status, status + " Client Error");
        }
        throw new RuntimeException(status + " Server Error");
    }

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData,
                                              String bindingName) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.name(), capability.name(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId, bindingName);
    }
}
