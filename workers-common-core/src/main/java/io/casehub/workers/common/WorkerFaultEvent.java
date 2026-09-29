package io.casehub.workers.common;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;

public record WorkerFaultEvent(
    CaseInstance caseInstance,
    Worker worker,
    Capability capability,
    String inputDataHash,
    String eventLogId,
    Throwable cause,
    String bindingName) {}
