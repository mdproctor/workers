package io.casehub.workers.common;

import io.casehub.worker.api.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;

public record WorkerCorrelationContext(
    CaseInstance caseInstance,
    Worker worker,
    String idempotency,
    String tenancyId,
    String bindingName
) {}
