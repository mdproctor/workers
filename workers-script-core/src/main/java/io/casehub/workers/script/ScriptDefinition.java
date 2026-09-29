package io.casehub.workers.script;

import java.util.List;
import java.util.Map;

public record ScriptDefinition(
    String name,
    String command,
    List<String> args,
    String workingDirectory,
    Map<String, String> environment,
    int timeoutSeconds,
    long maxOutputBytes
) {}
