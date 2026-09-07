package io.casehub.workers.scenario;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.casehub.workers.common.WorkerFaultEvent;
import io.casehub.workers.common.WorkerFaultHandler;
import io.casehub.workers.testing.WorkerTestSupport;
import org.junit.jupiter.api.Test;

class ScenarioWorkerFaultEventHandlerTest {

    @Test
    void onFault_delegatesToWorkerFaultHandler() {
        WorkerFaultHandler faultHandler = mock(WorkerFaultHandler.class);
        ScenarioWorkerFaultEventHandler handler = new ScenarioWorkerFaultEventHandler();
        handler.workerFaultHandler = faultHandler;

        WorkerFaultEvent event = new WorkerFaultEvent(
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "scenario:onboard"),
            WorkerTestSupport.testCapability("scenario:onboard"),
            "hash-1", "1",
            new RuntimeException("scenario failed"), null);

        handler.onFault(event);

        verify(faultHandler).handleFault(event);
    }
}
