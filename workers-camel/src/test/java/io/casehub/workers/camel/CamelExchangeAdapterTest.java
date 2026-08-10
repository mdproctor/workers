package io.casehub.workers.camel;

import io.casehub.worker.api.Exchange;
import io.casehub.worker.api.WorkerResult;
import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CamelExchangeAdapterTest {

    private static CamelContext camelContext;

    @BeforeAll
    static void setUp() {
        camelContext = new DefaultCamelContext();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (camelContext != null) {
            camelContext.close();
        }
    }

    @Test
    void toCamel_setsBodyAndHeaders() {
        Exchange<String> casehub = new Exchange<>("hello",
                Map.of("correlationId", "abc", "source", "system-a"),
                Map.of("loopCount", 3));

        org.apache.camel.Exchange camel = new DefaultExchange(camelContext);
        CamelExchangeAdapter.toCamel(casehub, camel);

        assertThat(camel.getIn().getBody()).isEqualTo("hello");
        assertThat(camel.getIn().getHeader("correlationId")).isEqualTo("abc");
        assertThat(camel.getIn().getHeader("source")).isEqualTo("system-a");
        assertThat(camel.getProperty("loopCount")).isEqualTo(3);
    }

    @Test
    void toCamel_filtersCamelInternalHeaders() {
        Exchange<String> casehub = new Exchange<>("data",
                Map.of("CamelInternal", "skip", "custom", "keep"),
                Map.of());

        org.apache.camel.Exchange camel = new DefaultExchange(camelContext);
        CamelExchangeAdapter.toCamel(casehub, camel);

        assertThat(camel.getIn().getHeader("custom")).isEqualTo("keep");
        assertThat(camel.getIn().getHeader("CamelInternal")).isNull();
    }

    @Test
    void fromCamel_extractsBodyAndHeaders() {
        org.apache.camel.Exchange camel = new DefaultExchange(camelContext);
        camel.getIn().setBody("processed");
        camel.getIn().setHeader("resultCode", 200);
        camel.getIn().setHeader("CamelHttpMethod", "POST");
        camel.setProperty("pipelineState", "done");

        Exchange<String> result = CamelExchangeAdapter.fromCamel(camel, String.class);

        assertThat(result.body()).isEqualTo("processed");
        assertThat(result.headers()).containsEntry("resultCode", 200);
        assertThat(result.headers()).doesNotContainKey("CamelHttpMethod");
        assertThat(result.properties()).containsEntry("pipelineState", "done");
    }

    @Test
    void toResult_onSuccess_returnsSuccessResult() {
        org.apache.camel.Exchange camel = new DefaultExchange(camelContext);
        camel.getIn().setBody("done");
        camel.getIn().setHeader("step", "final");

        WorkerResult<Exchange<String>> result = CamelExchangeAdapter.toResult(camel, String.class);

        assertThat(result.outcome()).isInstanceOf(io.casehub.worker.api.WorkerOutcome.Success.class);
        assertThat(result.output().body()).isEqualTo("done");
        assertThat(result.output().headers()).containsEntry("step", "final");
    }

    @Test
    void toResult_onException_returnsFailedResult() {
        org.apache.camel.Exchange camel = new DefaultExchange(camelContext);
        camel.setException(new RuntimeException("route failed"));

        WorkerResult<Exchange<String>> result = CamelExchangeAdapter.toResult(camel, String.class);

        assertThat(result.outcome()).isInstanceOf(io.casehub.worker.api.WorkerOutcome.Failed.class);
    }

    @Test
    void roundTrip_preservesData() {
        Exchange<Map<String, Object>> original = new Exchange<>(
                Map.of("key", "value"),
                Map.of("correlationId", "xyz"),
                Map.of("counter", 5));

        org.apache.camel.Exchange camel = new DefaultExchange(camelContext);
        CamelExchangeAdapter.toCamel(original, camel);

        @SuppressWarnings("unchecked")
        Exchange<Map<String, Object>> restored =
                (Exchange<Map<String, Object>>) (Exchange<?>) CamelExchangeAdapter.fromCamel(camel, Map.class);

        assertThat(restored.body()).isEqualTo(Map.of("key", "value"));
        assertThat(restored.headers()).containsEntry("correlationId", "xyz");
        assertThat(restored.properties()).containsEntry("counter", 5);
    }
}
