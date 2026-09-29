package io.casehub.workers.githubactions;

import io.casehub.workers.common.PermanentFaultException;

import java.util.Map;
import java.util.Optional;

public class GitHubActionsTokenResolver {

    private final Optional<String> globalToken;
    private final Map<String, String> orgTokens;
    private final String apiBaseUrl;

    public GitHubActionsTokenResolver(Optional<String> globalToken,
                                      Map<String, String> orgTokens,
                                      String apiBaseUrl) {
        this.globalToken = globalToken;
        this.orgTokens = orgTokens;
        this.apiBaseUrl = apiBaseUrl;
    }

    public String resolve(String owner) {
        String orgToken = orgTokens.get(owner);
        if (orgToken != null && !orgToken.isBlank()) {
            return orgToken;
        }
        return globalToken
            .filter(t -> !t.isBlank())
            .orElseThrow(() -> new PermanentFaultException(0,
                "No GitHub token configured for org '" + owner
                    + "' and no global fallback (casehub.workers.github-actions.token)"));
    }

    public boolean hasToken() {
        return globalToken.isPresent() && !globalToken.get().isBlank();
    }

    public String apiBaseUrl() {
        return apiBaseUrl;
    }
}
