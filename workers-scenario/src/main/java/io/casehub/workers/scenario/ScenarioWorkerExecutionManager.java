package io.casehub.workers.scenario;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerBackend;
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
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Map;

@WorkerBackend
@Priority(10)
@ApplicationScoped
public class ScenarioWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(ScenarioWorkerExecutionManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(30);

    @Inject ScenarioEndpointResolver endpointResolver;
    @Inject AsyncWorkerCompletionRegistry completionRegistry;
    @Inject WorkerFaultPublisher faultPublisher;
    @Inject io.vertx.mutiny.core.Vertx vertx;

    @ConfigProperty(name = "casehub.workers.callback-base-url", defaultValue = "")
    String callbackBaseUrl;

    WebClient webClient;

    @PostConstruct
    void init() {
        if (webClient == null) {
            webClient = WebClient.create(vertx);
        }
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
            faultPublisher.fault(ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT,
                ctx, capability, eventLogId,
                new PermanentFaultException(0, e.getMessage()));
            return;
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);

        try {
            String yaml = resolveYaml(inputData, endpoint);
            PendingCompletion pending = completionRegistry.register(
                ScenarioWorkerConstants.WORKER_TYPE,
                ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT,
                ctx, capability, eventLogId, DEFAULT_TTL, Map.of());

            dispatchGraphQL(endpoint, yaml, inputData, pending);
        } catch (Exception t) {
            faultPublisher.fault(ScenarioWorkerEventBusAddresses.SCENARIO_WORKER_FAULT,
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

        HttpResponse<Buffer> response = webClient.requestAbs(HttpMethod.GET, libraryUrl)
            .putHeader("Accept", "text/yaml")
            .timeout(endpoint.timeoutSeconds() * 1000L)
            .send()
            .await().indefinitely();

        int status = response.statusCode();

        if (status >= 200 && status < 300) {
            return response.bodyAsString();
        }
        if (status == 404) {
            throw new PermanentFaultException(404,
                "Scenario script not found: " + scriptName);
        }
        if (status == 429) {
            throw WorkerRetrySupport.parseRetryAfter(
                response.getHeader("Retry-After"), status, response.statusMessage());
        }
        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status,
                status + " " + response.statusMessage());
        }
        throw new RuntimeException(status + " " + response.statusMessage());
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

        HttpResponse<Buffer> response = webClient.requestAbs(HttpMethod.POST, endpoint.url())
            .putHeader("Content-Type", "application/json")
            .putHeader("Accept", "application/json")
            .timeout(endpoint.timeoutSeconds() * 1000L)
            .sendJson(body)
            .await().indefinitely();

        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            LOG.debugf("Scenario dispatched: %s → %s", pending.dispatchId(), endpoint.url());
            return;
        }
        if (status == 429) {
            throw WorkerRetrySupport.parseRetryAfter(
                response.getHeader("Retry-After"), status, response.statusMessage());
        }
        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status, status + " " + response.statusMessage());
        }
        throw new RuntimeException(status + " " + response.statusMessage());
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
