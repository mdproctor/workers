package io.casehub.workers.common;

public final class CasehubWorkerHeaders {
    public static final String WORKER_ID      = "casehub-worker-id";
    public static final String IDEMPOTENCY    = "casehub-idempotency";
    public static final String CASE_ID        = "casehub-case-id";
    public static final String TENANCY_ID     = "casehub-tenancy-id";
    public static final String TASK_TYPE      = "casehub-task-type";
    public static final String CALLBACK_TOKEN = "casehub-callback-token";
    public static final String WORK_STATUS    = "casehub-work-status";

    private CasehubWorkerHeaders() {}
}
