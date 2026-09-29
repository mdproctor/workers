package io.casehub.workers.common;

import io.casehub.engine.common.internal.event.WorkflowExecutionCompleted;

import java.util.Map;
import java.util.function.Consumer;

public class WorkflowCompletionPublisher {

    private final Consumer<WorkflowExecutionCompleted> completionConsumer;

    public WorkflowCompletionPublisher(
            Consumer<WorkflowExecutionCompleted> completionConsumer) {
        this.completionConsumer = completionConsumer;
    }

    public void complete(WorkerCorrelationContext ctx, Map<String, Object> output) {
        completionConsumer.accept(
            WorkflowExecutionCompleted.approved(
                ctx.caseInstance(), ctx.worker(), ctx.idempotency(), output, ctx.bindingName()));
    }
}
