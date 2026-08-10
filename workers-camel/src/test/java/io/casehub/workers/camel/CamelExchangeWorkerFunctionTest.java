package io.casehub.workers.camel;

import io.casehub.worker.api.Exchange;
import io.casehub.worker.api.ExchangeAwareFunction;
import io.casehub.worker.api.WorkerFunction;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CamelExchangeWorkerFunctionTest {

    @Test
    void implementsExchangeAwareFunction() {
        var fn = new CamelExchangeWorkerFunction<>(String.class, String.class, "direct:test");
        assertThat(fn).isInstanceOf(ExchangeAwareFunction.class);
        assertThat(fn).isInstanceOf(WorkerFunction.class);
    }

    @Test
    void bodyTypesAreAccessible() {
        var fn = new CamelExchangeWorkerFunction<>(String.class, Integer.class, "direct:enrich");
        assertThat(fn.bodyInputType()).isEqualTo(String.class);
        assertThat(fn.bodyOutputType()).isEqualTo(Integer.class);
    }

    @Test
    void inputOutputTypesAreExchangeClass() {
        var fn = new CamelExchangeWorkerFunction<>(Map.class, Map.class, "direct:process");
        assertThat(fn.inputType()).isEqualTo(Exchange.class);
        assertThat(fn.outputType()).isEqualTo(Exchange.class);
    }

    @Test
    void routeUriIsAccessible() {
        var fn = new CamelExchangeWorkerFunction<>(String.class, String.class, "direct:myRoute");
        assertThat(fn.routeUri()).isEqualTo("direct:myRoute");
    }
}
