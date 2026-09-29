package io.casehub.workers.k8s;

public final class K8sWorkerConstants {
    public static final String WORKER_TYPE = "k8s";
    public static final String TAG_PREFIX = "k8s:";
    public static final String MANAGED_BY_LABEL = "app.kubernetes.io/managed-by";
    public static final String MANAGED_BY_VALUE = "casehub";
    public static final String DISPATCH_ID_LABEL = "casehub.io/dispatch-id";
    public static final String CAPABILITY_LABEL = "casehub.io/capability";
    public static final String TENANCY_ID_LABEL = "casehub.io/tenancy-id";
    public static final String CASE_ID_LABEL = "casehub.io/case-id";
    public static final String WORKER_NAME_LABEL = "casehub.io/worker-name";
    public static final String EVENT_LOG_ID_LABEL = "casehub.io/event-log-id";
    public static final String IDEMPOTENCY_LABEL = "casehub.io/idempotency";
    public static final String BINDING_NAME_ANNOTATION = "casehub.io/binding-name";

    private K8sWorkerConstants() {}
}
