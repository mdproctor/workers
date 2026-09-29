package io.casehub.workers.script;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.WorkerProvisioningException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ScriptDefinitionResolverTest {

    @Test
    void initialize_parsesConfigIntoDefinitions() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "data-pipeline", new ScriptDefinition(
                "data-pipeline", "python3",
                List.of("/opt/scripts/pipeline.py", "--mode", "batch"),
                "/opt/data",
                Map.of("PYTHONPATH", "/opt/lib"),
                600, 2_097_152)
        ));

        assertThat(resolver.capabilities()).containsExactly("script:data-pipeline");
    }

    @Test
    void resolve_knownTag_returnsDefinition() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "run-tests", new ScriptDefinition(
                "run-tests", "/bin/sh",
                List.of("-c", "echo test"), null,
                Map.of(), 300, 1_048_576)
        ));

        ScriptDefinition def = resolver.resolve("script:run-tests", "tenant-1");

        assertThat(def.name()).isEqualTo("run-tests");
        assertThat(def.command()).isEqualTo("/bin/sh");
        assertThat(def.args()).containsExactly("-c", "echo test");
    }

    @Test
    void resolve_unknownTag_throws() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of());

        assertThatThrownBy(() -> resolver.resolve("script:missing", "tenant-1"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void resolve_tagWithoutPrefix_throws() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "run-tests", new ScriptDefinition(
                "run-tests", "/bin/sh",
                List.of(), null, Map.of(), 300, 1_048_576)
        ));

        assertThatThrownBy(() -> resolver.resolve("run-tests", "tenant-1"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void firstMatch_returnsFirstKnownCapability() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "build", new ScriptDefinition(
                "build", "make", List.of(), null, Map.of(), 300, 1_048_576)
        ));

        assertThat(resolver.firstMatch(Set.of("script:build", "script:unknown"), "tenant-1"))
            .contains("script:build");
    }

    @Test
    void firstMatch_noMatch_returnsEmpty() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of());

        assertThat(resolver.firstMatch(Set.of("script:unknown"), "tenant-1")).isEmpty();
    }

    @Test
    void capabilities_returnsAllPrefixedTags() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver(300, 1_048_576);
        resolver.initialize(Map.of(
            "a", new ScriptDefinition("a", "cmd", List.of(), null, Map.of(), 300, 1_048_576),
            "b", new ScriptDefinition("b", "cmd", List.of(), null, Map.of(), 300, 1_048_576)
        ));

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("script:a", "script:b");
    }
}
