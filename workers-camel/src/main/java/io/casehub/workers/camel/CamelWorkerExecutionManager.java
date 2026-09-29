package io.casehub.workers.camel;

import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerBackend;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.Map;

@WorkerBackend
@Priority(10)
@ApplicationScoped
public class CamelWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(CamelWorkerExecutionManager.class);

    @Inject
    CamelCapabilityResolver       camelCapabilityResolver;
    @Inject
    WorkerFaultPublisher          faultPublisher;
    @Inject
    AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry;
    @Inject
    WorkflowCompletionPublisher   completionPublisher;
    @Inject
    ProducerTemplate              producerTemplate;

    @ConfigProperty(name = "casehub.workers.async.timeout-minutes", defaultValue = "60")
    int asyncTimeoutMinutes;

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData) {
        submit(eventLogId, instance, worker, capability, inputData, null);
    }

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData,
                       String bindingName) {
        String entryUri;
        try {
            entryUri = camelCapabilityResolver.resolve(capability.name(), instance.tenancyId);
        } catch (WorkerProvisioningException e) {
            LOG.errorf("Camel route for capability %s missing at dispatch time", capability.name());
            faultPublisher.fault(
                    buildCtx(instance, worker, capability, inputData, bindingName),
                    capability, eventLogId, e);
            return;
        }

        WorkerCorrelationContext ctx     = buildCtx(instance, worker, capability, inputData, bindingName);
        ExchangePattern          pattern = camelCapabilityResolver.exchangePattern(capability.name());

        if (pattern == ExchangePattern.InOut) {
            submitSync(ctx, entryUri, capability, inputData, eventLogId);
        } else {
            submitAsync(ctx, entryUri, capability, eventLogId, inputData);
        }
    }

    private void submitSync(WorkerCorrelationContext ctx, String entryUri,
                            Capability capability, Map<String, Object> inputData,
                            Long eventLogId) {
        try {
            Exchange response = producerTemplate.request(entryUri, exchange -> {
                exchange.getIn().setHeader(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency());
                exchange.getIn().setHeader(CasehubWorkerHeaders.CASE_ID,
                                           ctx.caseInstance().getUuid().toString());
                exchange.getIn().setHeader(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId());
                exchange.getIn().setHeader(CasehubWorkerHeaders.TASK_TYPE, capability.name());
                exchange.getIn().setBody(inputData);
            });

            boolean faulted = response.getException() != null
                              || "FAULTED".equals(response.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS));
            if (faulted) {
                faultPublisher.fault(ctx, capability, eventLogId, response.getException());
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> output = response.getIn().getBody(Map.class);
                completionPublisher.complete(ctx, output != null ? output : Map.of());
            }
        } catch (Exception t) {
            faultPublisher.fault(ctx, capability, eventLogId, t);
        }
    }

    private void submitAsync(WorkerCorrelationContext ctx, String entryUri,
                             Capability capability, Long eventLogId,
                             Map<String, Object> inputData) {
        PendingCompletion pending = asyncWorkerCompletionRegistry.register(
                CamelWorkerConstants.WORKER_TYPE,
                ctx, capability, eventLogId,
                Duration.ofMinutes(asyncTimeoutMinutes), Map.of());

        producerTemplate.send(entryUri, exchange -> {
            exchange.getIn().setHeader(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency());
            exchange.getIn().setHeader(CasehubWorkerHeaders.CASE_ID,
                                       ctx.caseInstance().getUuid().toString());
            exchange.getIn().setHeader(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId());
            exchange.getIn().setHeader(CasehubWorkerHeaders.TASK_TYPE, capability.name());
            exchange.getIn().setHeader(CasehubWorkerHeaders.WORKER_ID, pending.dispatchId());
            exchange.getIn().setHeader(CasehubWorkerHeaders.CALLBACK_TOKEN, pending.callbackToken());
            exchange.getIn().setBody(inputData);
        });
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
        return camelCapabilityResolver.canResolve(capabilityName, tenancyId);
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return asyncWorkerCompletionRegistry.countByWorkerName(workerId);
    }
}
