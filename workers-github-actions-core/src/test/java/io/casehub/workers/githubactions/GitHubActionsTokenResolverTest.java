package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.PermanentFaultException;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GitHubActionsTokenResolverTest {

    @Test
    void globalToken_resolves() {
        var resolver = new GitHubActionsTokenResolver(
            Optional.of("ghp_global"), Map.of(), "https://api.github.com");

        assertThat(resolver.resolve("any-org")).isEqualTo("ghp_global");
    }

    @Test
    void perOrgToken_takesPrecedence() {
        var resolver = new GitHubActionsTokenResolver(
            Optional.of("ghp_global"), Map.of("casehubio", "ghp_casehub"), "https://api.github.com");

        assertThat(resolver.resolve("casehubio")).isEqualTo("ghp_casehub");
    }

    @Test
    void perOrgMiss_fallsBackToGlobal() {
        var resolver = new GitHubActionsTokenResolver(
            Optional.of("ghp_global"), Map.of("other-org", "ghp_other"), "https://api.github.com");

        assertThat(resolver.resolve("casehubio")).isEqualTo("ghp_global");
    }

    @Test
    void noToken_throwsPermanentFault() {
        var resolver = new GitHubActionsTokenResolver(
            Optional.empty(), Map.of(), "https://api.github.com");

        assertThatThrownBy(() -> resolver.resolve("casehubio"))
            .isInstanceOf(PermanentFaultException.class)
            .hasMessageContaining("casehubio");
    }

    @Test
    void noGlobal_perOrgMiss_throwsPermanentFault() {
        var resolver = new GitHubActionsTokenResolver(
            Optional.empty(), Map.of("other-org", "ghp_other"), "https://api.github.com");

        assertThatThrownBy(() -> resolver.resolve("casehubio"))
            .isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void hasToken_globalExists() {
        var resolver = new GitHubActionsTokenResolver(
            Optional.of("ghp_global"), Map.of(), "https://api.github.com");

        assertThat(resolver.hasToken()).isTrue();
    }

    @Test
    void hasToken_noGlobal() {
        var resolver = new GitHubActionsTokenResolver(
            Optional.empty(), Map.of(), "https://api.github.com");

        assertThat(resolver.hasToken()).isFalse();
    }
}
