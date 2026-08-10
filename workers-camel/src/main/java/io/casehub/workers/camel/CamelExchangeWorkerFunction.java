package io.casehub.workers.camel;

import io.casehub.worker.api.Exchange;
import io.casehub.worker.api.ExchangeAwareFunction;

public record CamelExchangeWorkerFunction<T, R>(
    Class<T> bodyInputType,
    Class<R> bodyOutputType,
    String routeUri
) implements ExchangeAwareFunction<T, R> {

    @Override
    @SuppressWarnings("unchecked")
    public Class<Exchange<T>> inputType() {
        return (Class) Exchange.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Exchange<R>> outputType() {
        return (Class) Exchange.class;
    }
}
