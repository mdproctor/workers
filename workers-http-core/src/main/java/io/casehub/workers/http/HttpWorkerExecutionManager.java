package io.casehub.workers.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkflowCompletionPublisher;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(HttpWorkerExecutionManager.class.getName());
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Pattern URI_TEMPLATE_PATTERN = Pattern.compile("\\{(\\w+)\\}");

    private final HttpEndpointResolver httpEndpointResolver;
    private final WorkerFaultPublisher faultPublisher;
    private final AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry;
    private final WorkflowCompletionPublisher completionPublisher;
    private final ObjectMapper objectMapper;
    private final int asyncTimeoutMinutes;

    HttpClient httpClient = HttpClient.newHttpClient();

    public HttpWorkerExecutionManager(HttpEndpointResolver httpEndpointResolver,
                                       WorkerFaultPublisher faultPublisher,
                                       AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry,
                                       WorkflowCompletionPublisher completionPublisher,
                                       ObjectMapper objectMapper,
                                       int asyncTimeoutMinutes) {
        this.httpEndpointResolver = httpEndpointResolver;
        this.faultPublisher = faultPublisher;
        this.asyncWorkerCompletionRegistry = asyncWorkerCompletionRegistry;
        this.completionPublisher = completionPublisher;
        this.objectMapper = objectMapper;
        this.asyncTimeoutMinutes = asyncTimeoutMinutes;
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
        ResolvedEndpoint endpoint;
        try {
            endpoint = httpEndpointResolver.resolve(capability.name(), instance.tenancyId);
        } catch (WorkerProvisioningException e) {
            LOG.severe("HTTP endpoint for capability " + capability.name() + " missing at dispatch time");
            faultPublisher.fault(
                    buildCtx(instance, worker, capability, inputData, bindingName),
                    capability, eventLogId, e);
            return;
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);

        String resolvedUrl;
        try {
            resolvedUrl = interpolateUrl(endpoint.url(), inputData);
        } catch (PermanentFaultException e) {
            faultPublisher.fault(ctx, capability, eventLogId, e);
            return;
        }

        if (endpoint.mode() == ExchangeMode.SYNC) {
            submitSync(ctx, endpoint, resolvedUrl, capability, inputData, eventLogId);
        } else {
            submitAsync(ctx, endpoint, resolvedUrl, capability, inputData, eventLogId);
        }
    }

    private void submitSync(WorkerCorrelationContext ctx, ResolvedEndpoint endpoint,
                            String resolvedUrl, Capability capability,
                            Map<String, Object> inputData, Long eventLogId) {
        try {
            byte[] jsonBody = objectMapper.writeValueAsBytes(inputData);
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(resolvedUrl))
                .method(endpoint.method(), HttpRequest.BodyPublishers.ofByteArray(jsonBody))
                .header("Content-Type", "application/json")
                .header(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency())
                .header(CasehubWorkerHeaders.CASE_ID, ctx.caseInstance().getUuid().toString())
                .header(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId())
                .header(CasehubWorkerHeaders.TASK_TYPE, capability.name())
                .timeout(Duration.ofSeconds(endpoint.timeoutSeconds()));
            endpoint.headers().forEach(builder::header);
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            handleResponse(ctx, response);
        } catch (Exception t) {
            faultPublisher.fault(ctx, capability, eventLogId, t);
        }
    }

    private void submitAsync(WorkerCorrelationContext ctx, ResolvedEndpoint endpoint,
                             String resolvedUrl, Capability capability,
                             Map<String, Object> inputData, Long eventLogId) {
        PendingCompletion pending = asyncWorkerCompletionRegistry.register(
                HttpWorkerConstants.WORKER_TYPE,
                ctx, capability, eventLogId,
                Duration.ofMinutes(asyncTimeoutMinutes), Map.of());

        try {
            byte[] jsonBody = objectMapper.writeValueAsBytes(inputData);
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(resolvedUrl))
                .method(endpoint.method(), HttpRequest.BodyPublishers.ofByteArray(jsonBody))
                .header("Content-Type", "application/json")
                .header(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency())
                .header(CasehubWorkerHeaders.CASE_ID, ctx.caseInstance().getUuid().toString())
                .header(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId())
                .header(CasehubWorkerHeaders.TASK_TYPE, capability.name())
                .header(CasehubWorkerHeaders.WORKER_ID, pending.dispatchId())
                .header(CasehubWorkerHeaders.CALLBACK_TOKEN, pending.callbackToken())
                .timeout(Duration.ofSeconds(endpoint.timeoutSeconds()));
            endpoint.headers().forEach(builder::header);
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                return;
            }
            if (status == 429) {
                String retryAfter = response.headers().firstValue("Retry-After").orElse(null);
                RuntimeException ex = WorkerRetrySupport.parseRetryAfter(retryAfter, status, "Too Many Requests");
                if (ex instanceof RetryAfterException ra) {
                    long remainingMs = java.time.Duration.between(
                            java.time.Instant.now(), pending.expiresAt()).toMillis();
                    long capped = Math.min(ra.retryAfterMs(), Math.max(0, remainingMs));
                    throw new RetryAfterException(capped, ra.getMessage());
                }
                throw ex;
            }
            if (status >= 400 && status < 500) {
                throw new PermanentFaultException(status, status + " Client Error");
            }
            throw new RuntimeException(status + " Server Error");
        } catch (Exception t) {
            faultPublisher.fault(ctx, capability, eventLogId, t);
        }
    }

    private void handleResponse(WorkerCorrelationContext ctx, HttpResponse<String> response) {
        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            Map<String, Object> output = deserializeBody(response);
            completionPublisher.complete(ctx, output);
            return;
        }
        if (status == 429) {
            String retryAfter = response.headers().firstValue("Retry-After").orElse(null);
            throw WorkerRetrySupport.parseRetryAfter(retryAfter, status, "Too Many Requests");
        }
        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status, status + " Client Error");
        }
        throw new RuntimeException(status + " Server Error");
    }

    private Map<String, Object> deserializeBody(HttpResponse<String> response) {
        String body = response.body();
        if (body == null || body.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(body, MAP_TYPE);
        } catch (Exception e) {
            return Map.of();
        }
    }

    static String interpolateUrl(String urlTemplate, Map<String, Object> inputData) {
        Matcher matcher = URI_TEMPLATE_PATTERN.matcher(urlTemplate);
        if (!matcher.find()) {
            return urlTemplate;
        }
        StringBuilder sb = new StringBuilder();
        matcher.reset();
        while (matcher.find()) {
            String key = matcher.group(1);
            Object value = inputData.get(key);
            if (value == null) {
                throw new PermanentFaultException(0,
                    "URI template variable {" + key + "} not found in inputData");
            }
            matcher.appendReplacement(sb,
                Matcher.quoteReplacement(
                    URLEncoder.encode(value.toString(), StandardCharsets.UTF_8)));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData,
                                              String bindingName) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.name(), capability.name(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId, bindingName);
    }

    @Override
    public boolean supports(String capabilityName, String tenancyId) {
        return httpEndpointResolver.canResolve(capabilityName, tenancyId);
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return asyncWorkerCompletionRegistry.countByWorkerName(workerId);
    }
}
