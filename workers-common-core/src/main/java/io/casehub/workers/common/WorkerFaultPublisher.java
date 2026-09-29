package io.casehub.workers.common;

import io.casehub.worker.api.Capability;

import java.util.function.Consumer;

public class WorkerFaultPublisher {

    private final Consumer<WorkerFaultEvent> faultConsumer;

    public WorkerFaultPublisher(Consumer<WorkerFaultEvent> faultConsumer) {
        this.faultConsumer = faultConsumer;
    }

    public void fault(WorkerCorrelationContext ctx,
                      Capability capability, Long eventLogId, Throwable cause) {
        faultConsumer.accept(new WorkerFaultEvent(
            ctx.caseInstance(), ctx.worker(), capability,
            ctx.idempotency(), eventLogId.toString(), cause, ctx.bindingName()));
    }

    public void fault(PendingCompletion pending, Throwable cause) {
        faultConsumer.accept(new WorkerFaultEvent(
            pending.correlationContext().caseInstance(),
            pending.correlationContext().worker(),
            pending.capability(),
            pending.correlationContext().idempotency(),
            pending.eventLogId().toString(),
            cause,
            pending.correlationContext().bindingName()));
    }
}
