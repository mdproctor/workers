package io.casehub.workers.camel;

import io.casehub.worker.api.Exchange;
import io.casehub.worker.api.WorkerResult;

import java.util.LinkedHashMap;
import java.util.Map;

public final class CamelExchangeAdapter {

    private CamelExchangeAdapter() {}

    public static void toCamel(Exchange<?> casehubExchange,
                                org.apache.camel.Exchange camelExchange) {
        camelExchange.getIn().setBody(casehubExchange.body());
        casehubExchange.headers().forEach((k, v) -> {
            if (!k.startsWith("Camel")) {
                camelExchange.getIn().setHeader(k, v);
            }
        });
        casehubExchange.properties().forEach(camelExchange::setProperty);
    }

    public static <R> Exchange<R> fromCamel(org.apache.camel.Exchange camelExchange,
                                             Class<R> bodyType) {
        R body = camelExchange.getIn().getBody(bodyType);
        Map<String, Object> headers = new LinkedHashMap<>(camelExchange.getIn().getHeaders());
        Map<String, Object> properties = new LinkedHashMap<>(camelExchange.getProperties());
        headers.entrySet().removeIf(e -> e.getKey().startsWith("Camel"));
        return new Exchange<>(body, headers, properties);
    }

    public static <R> WorkerResult<Exchange<R>> toResult(
            org.apache.camel.Exchange camelExchange, Class<R> bodyType) {
        if (camelExchange.getException() != null) {
            return WorkerResult.failed(camelExchange.getException().getMessage());
        }
        return WorkerResult.of(fromCamel(camelExchange, bodyType));
    }
}
