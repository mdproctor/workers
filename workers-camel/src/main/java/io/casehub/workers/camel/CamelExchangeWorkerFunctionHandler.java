package io.casehub.workers.camel;

import io.casehub.api.model.WorkerContext;
import io.casehub.engine.common.internal.executor.ExecutionMetadata;
import io.casehub.engine.common.internal.executor.HandlerResult;
import io.casehub.engine.common.internal.executor.WorkerFunctionHandler;
import io.casehub.worker.api.Exchange;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.ProducerTemplate;

import java.util.Map;

@ApplicationScoped
public class CamelExchangeWorkerFunctionHandler implements WorkerFunctionHandler {

    private final ProducerTemplate producerTemplate;

    @Inject
    public CamelExchangeWorkerFunctionHandler(ProducerTemplate producerTemplate) {
        this.producerTemplate = producerTemplate;
    }

    @Override
    public boolean supports(WorkerFunction<?, ?> function) {
        return function instanceof CamelExchangeWorkerFunction<?, ?>;
    }

    @Override
    @SuppressWarnings("unchecked")
    public HandlerResult execute(
            WorkerFunction<?, ?> function,
            Object inputData,
            WorkerContext context,
            int timeoutMs,
            ExecutionMetadata metadata) {

        CamelExchangeWorkerFunction<?, ?> camelFn = (CamelExchangeWorkerFunction<?, ?>) function;
        Exchange<?> inputExchange = (Exchange<?>) inputData;

        org.apache.camel.Exchange camelExchange = producerTemplate.getCamelContext()
                .getEndpoint(camelFn.routeUri())
                .createExchange();

        CamelExchangeAdapter.toCamel(inputExchange, camelExchange);

        org.apache.camel.Exchange result = producerTemplate.send(camelFn.routeUri(), camelExchange);

        WorkerResult<Exchange<?>> workerResult =
                (WorkerResult) CamelExchangeAdapter.toResult(result, camelFn.bodyOutputType());

        Map<String, Object> protocolMetadata = Map.of(
                "camelRouteUri", camelFn.routeUri(),
                "camelExchangeId", result.getExchangeId());

        return new HandlerResult(workerResult, protocolMetadata);
    }
}
