package io.casehub.workers.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.WorkerProvisioningException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JobDefinitionResolverTest {

    private static JobDefinitionResolver defaultResolver() {
        return new JobDefinitionResolver("default", 3600, 600, 0, "delete", 1_048_576, 262_144);
    }

    private static JobDefinition imageDef(String name, String namespace) {
        return new JobDefinition(name, namespace, "acme/" + name + ":latest",
            List.of(), List.of(), null, null, null, null, null,
            3600, 600, 0, 1_048_576, null, Map.of(), Map.of(), CleanupPolicy.DELETE);
    }

    private static JobDefinition templateDef(String name, String namespace) {
        return new JobDefinition(name, namespace, null,
            List.of(), List.of(), "jobs/" + name + ".yaml", null, null, null, null,
            3600, 600, 0, 1_048_576, null, Map.of(), Map.of(), CleanupPolicy.DELETE);
    }

    @Test
    void initialize_buildsCapabilitiesFromNames() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of(
            "report-gen", imageDef("report-gen", "batch"),
            "ml-inference", templateDef("ml-inference", "ml")
        ));

        assertThat(resolver.capabilities())
            .containsExactlyInAnyOrder("k8s:report-gen", "k8s:ml-inference");
    }

    @Test
    void resolve_knownTag_returnsDefinition() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of("report-gen", imageDef("report-gen", "batch")));

        JobDefinition def = resolver.resolve("k8s:report-gen", "tenant-1");

        assertThat(def.name()).isEqualTo("report-gen");
        assertThat(def.image()).isEqualTo("acme/report-gen:latest");
        assertThat(def.namespace()).isEqualTo("batch");
    }

    @Test
    void resolve_unknownTag_throws() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of());

        assertThatThrownBy(() -> resolver.resolve("k8s:missing", "tenant-1"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void resolve_tagWithoutPrefix_throws() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of("report-gen", imageDef("report-gen", "batch")));

        assertThatThrownBy(() -> resolver.resolve("report-gen", "tenant-1"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void firstMatch_returnsFirstKnownCapability() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of("build", imageDef("build", "ci")));

        assertThat(resolver.firstMatch(Set.of("k8s:build", "k8s:unknown"), "tenant-1"))
            .contains("k8s:build");
    }

    @Test
    void firstMatch_noMatch_returnsEmpty() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of());

        assertThat(resolver.firstMatch(Set.of("k8s:unknown"), "tenant-1")).isEmpty();
    }

    @Test
    void capabilities_returnsAllPrefixedTags() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of(
            "a", imageDef("a", "ns"),
            "b", imageDef("b", "ns")
        ));

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("k8s:a", "k8s:b");
    }

    @Test
    void namespaces_returnsUniqueNamespacesFromAllDefinitions() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of(
            "a", imageDef("a", "batch"),
            "b", imageDef("b", "ml"),
            "c", imageDef("c", "batch")
        ));

        assertThat(resolver.namespaces()).containsExactlyInAnyOrder("batch", "ml");
    }

    @Test
    void canResolve_delegatesToCapabilities() {
        JobDefinitionResolver resolver = defaultResolver();
        resolver.initialize(Map.of("x", imageDef("x", "ns")));

        assertThat(resolver.canResolve("k8s:x", "t1")).isTrue();
        assertThat(resolver.canResolve("k8s:y", "t1")).isFalse();
        assertThat(resolver.canResolve("script:x", "t1")).isFalse();
    }

    @Test
    void buildFromConfig_imageJob() {
        JobDefinitionResolver resolver = defaultResolver();
        JobDefinition def = resolver.buildFromConfig("report-gen", Map.of(
            "image", "acme/report:latest",
            "namespace", "batch",
            "timeout-seconds", "120"
        ));

        assertThat(def.name()).isEqualTo("report-gen");
        assertThat(def.image()).isEqualTo("acme/report:latest");
        assertThat(def.namespace()).isEqualTo("batch");
        assertThat(def.timeoutSeconds()).isEqualTo(120);
    }

    @Test
    void buildFromConfig_templateJob() {
        JobDefinitionResolver resolver = defaultResolver();
        JobDefinition def = resolver.buildFromConfig("ml", Map.of(
            "template", "jobs/ml.yaml"
        ));

        assertThat(def.name()).isEqualTo("ml");
        assertThat(def.template()).isEqualTo("jobs/ml.yaml");
        assertThat(def.namespace()).isEqualTo("default");
    }

    @Test
    void buildFromConfig_noImageOrTemplate_throws() {
        JobDefinitionResolver resolver = defaultResolver();

        assertThatThrownBy(() -> resolver.buildFromConfig("empty", Map.of()))
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("image");
    }
}
