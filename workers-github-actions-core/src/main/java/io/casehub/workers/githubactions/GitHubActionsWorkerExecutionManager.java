package io.casehub.workers.githubactions;

import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

public class GitHubActionsWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(GitHubActionsWorkerExecutionManager.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final GitHubActionsTokenResolver tokenResolver;
    private final WorkerFaultPublisher faultPublisher;
    private final WorkflowCompletionPublisher completionPublisher;

    HttpClient httpClient = HttpClient.newHttpClient();

    public GitHubActionsWorkerExecutionManager(GitHubActionsTokenResolver tokenResolver,
                                                WorkerFaultPublisher faultPublisher,
                                                WorkflowCompletionPublisher completionPublisher) {
        this.tokenResolver = tokenResolver;
        this.faultPublisher = faultPublisher;
        this.completionPublisher = completionPublisher;
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
        String  capTag             = capability.name();
        boolean isWorkflowDispatch = GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH.equals(capTag);

        String owner = stringField(inputData, "owner");
        String repo  = stringField(inputData, "repo");
        if (owner == null || repo == null) {
            faultPublisher.fault(buildCtx(instance, worker, capability, inputData, bindingName),
                                 capability, eventLogId,
                                 new PermanentFaultException(0, "Missing required inputData: owner, repo"));
            return;
        }

        String              url;
        Map<String, Object> body;

        if (isWorkflowDispatch) {
            String workflowId = stringField(inputData, "workflow_id");
            String ref        = stringField(inputData, "ref");
            if (workflowId == null || ref == null) {
                faultPublisher.fault(buildCtx(instance, worker, capability, inputData, bindingName),
                                     capability, eventLogId,
                                     new PermanentFaultException(0,
                                                                 "Missing required inputData for workflow-dispatch: workflow_id, ref"));
                return;
            }
            url  = tokenResolver.apiBaseUrl() + "/repos/" + owner + "/" + repo
                   + "/actions/workflows/" + workflowId + "/dispatches";
            body = new LinkedHashMap<>();
            body.put("ref", ref);
            Object inputs = inputData.get("inputs");
            if (inputs != null) {
                body.put("inputs", inputs);
            }
        } else {
            String eventType = stringField(inputData, "event_type");
            if (eventType == null) {
                faultPublisher.fault(buildCtx(instance, worker, capability, inputData, bindingName),
                                     capability, eventLogId,
                                     new PermanentFaultException(0,
                                                                 "Missing required inputData for repository-dispatch: event_type"));
                return;
            }
            url  = tokenResolver.apiBaseUrl() + "/repos/" + owner + "/" + repo + "/dispatches";
            body = new LinkedHashMap<>();
            body.put("event_type", eventType);
            Object clientPayload = inputData.get("client_payload");
            if (clientPayload != null) {
                body.put("client_payload", clientPayload);
            }
        }

        String token;
        try {
            token = tokenResolver.resolve(owner);
        } catch (PermanentFaultException e) {
            faultPublisher.fault(buildCtx(instance, worker, capability, inputData, bindingName),
                                 capability, eventLogId, e);
            return;
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);

        try {
            byte[] jsonBody = OBJECT_MAPPER.writeValueAsBytes(body);
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofByteArray(jsonBody))
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header("Content-Type", "application/json")
                .timeout(REQUEST_TIMEOUT)
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                completionPublisher.complete(ctx, Map.of(
                        "dispatched", true, "owner", owner, "repo", repo));
                return;
            }
            if (status == 422) {
                if (isWorkflowDispatch) {
                    throw new RetryAfterException(60_000,
                                                  "422 — workflow_dispatch trigger may be cached (GE-20260426-805acb)");
                } else {
                    throw new PermanentFaultException(status, status + " Unprocessable Entity");
                }
            }
            if (status == 429) {
                throw WorkerRetrySupport.parseRetryAfter(
                        response.headers().firstValue("Retry-After").orElse(null), status, "Too Many Requests");
            }
            if (status >= 400 && status < 500) {
                throw new PermanentFaultException(status, status + " Client Error");
            }
            throw new RuntimeException(status + " Server Error");
        } catch (JsonProcessingException e) {
            faultPublisher.fault(ctx, capability, eventLogId,
                new PermanentFaultException(0, "Failed to serialize request: " + e.getMessage()));
        } catch (Exception t) {
            faultPublisher.fault(ctx, capability, eventLogId, t);
        }
    }

    @Override
    public boolean supports(String capabilityName, String tenancyId) {
        return GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH.equals(capabilityName)
            || GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH.equals(capabilityName);
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return 0;
    }

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData,
                                              String bindingName) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.name(), capability.name(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId, bindingName);
    }

    private static String stringField(Map<String, Object> data, String key) {
        Object val = data.get(key);
        if (val == null) return null;
        String s = val.toString();
        return s.isBlank() ? null : s;
    }
}
