package io.casehub.workers.script;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import java.util.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ScriptWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(ScriptWorkerExecutionManager.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final int READ_BUFFER_SIZE = 8192;

    private final ScriptDefinitionResolver scriptDefinitionResolver;
    private final WorkerFaultPublisher faultPublisher;
    private final WorkflowCompletionPublisher completionPublisher;
    private final ExecutorService streamDrainExecutor;

    public ScriptWorkerExecutionManager(ScriptDefinitionResolver scriptDefinitionResolver,
                                        WorkerFaultPublisher faultPublisher,
                                        WorkflowCompletionPublisher completionPublisher) {
        this.scriptDefinitionResolver = scriptDefinitionResolver;
        this.faultPublisher = faultPublisher;
        this.completionPublisher = completionPublisher;
        this.streamDrainExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "script-stream-drain");
            t.setDaemon(true);
            return t;
        });
    }

    public void shutdown() {
        streamDrainExecutor.shutdownNow();
    }

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData) {
        submit(eventLogId, instance, worker, capability, inputData, null);
    }

    @Override
    public void submit(Long eventLogId, CaseInstance instance, Worker worker,
                       Capability capability, Map<String, Object> inputData,
                       String bindingName) {
        ScriptDefinition definition;
        try {
            definition = scriptDefinitionResolver.resolve(capability.name(), instance.tenancyId);
        } catch (WorkerProvisioningException e) {
            WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);
            faultPublisher.fault(
                                 ctx, capability, eventLogId,
                                 new PermanentFaultException(0, e.getMessage()));
            return;
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData, bindingName);

        try {
            Map<String, Object> output = executeProcess(definition, inputData, ctx, capability);
            completionPublisher.complete(ctx, output);
        } catch (Exception t) {
            faultPublisher.fault(
                                 ctx, capability, eventLogId, t);
        }
    }

    @Override
    public boolean supports(String capabilityName, String tenancyId) {
        return scriptDefinitionResolver.canResolve(capabilityName, tenancyId);
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return 0;
    }

    Map<String, Object> executeProcess(ScriptDefinition definition,
                                        Map<String, Object> inputData,
                                        WorkerCorrelationContext ctx,
                                        Capability capability) {
        List<String> command = new ArrayList<>();
        command.add(definition.command());
        command.addAll(definition.args());

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(false);

        if (definition.workingDirectory() != null) {
            pb.directory(new File(definition.workingDirectory()));
        }

        Map<String, String> env = pb.environment();
        if (definition.environment() != null) {
            env.putAll(definition.environment());
        }
        env.put("CASEHUB_CASE_ID", ctx.caseInstance().getUuid().toString());
        env.put("CASEHUB_TENANCY_ID", ctx.tenancyId() != null ? ctx.tenancyId() : "");
        env.put("CASEHUB_CAPABILITY", capability.name());
        env.put("CASEHUB_IDEMPOTENCY", ctx.idempotency());

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            throw new PermanentFaultException(0, "Command not found: " + definition.command());
        }

        CompletableFuture<String> stderrFuture = CompletableFuture.supplyAsync(
            () -> drainStream(process.getErrorStream(), definition.maxOutputBytes()),
            streamDrainExecutor);

        CompletableFuture<String> stdoutFuture = CompletableFuture.supplyAsync(
            () -> drainStream(process.getInputStream(), definition.maxOutputBytes()),
            streamDrainExecutor);

        try {
            byte[] inputJson = OBJECT_MAPPER.writeValueAsBytes(inputData);
            try (OutputStream stdin = process.getOutputStream()) {
                stdin.write(inputJson);
            }
        } catch (IOException e) {
            if (!process.isAlive()) {
                LOG.fine("Stdin write failed after process exited (broken pipe) — ignoring");
            } else {
                process.destroyForcibly();
                throw new RuntimeException("Failed to write inputData to stdin: " + e.getMessage());
            }
        }

        boolean completed;
        try {
            completed = process.waitFor(definition.timeoutSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            throw new RuntimeException("Interrupted waiting for process");
        }

        if (!completed) {
            process.destroyForcibly();
            try { process.waitFor(); } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            throw new PermanentFaultException(0,
                "Process timed out after " + definition.timeoutSeconds() + "s");
        }

        String stdout;
        try {
            stdout = stdoutFuture.join();
        } catch (Exception e) {
            stdout = "";
        }

        String stderr;
        try {
            stderr = stderrFuture.join();
        } catch (Exception e) {
            stderr = "";
        }

        int exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new RuntimeException("Process exited with code " + exitCode
                + ": " + (stderr.isBlank() ? stdout : stderr).trim());
        }

        return parseOutput(stdout, stderr, exitCode);
    }

    private Map<String, Object> parseOutput(String stdout, String stderr, int exitCode) {
        String trimmed = stdout.trim();
        if (!trimmed.isEmpty() && trimmed.startsWith("{")) {
            try {
                return OBJECT_MAPPER.readValue(trimmed, MAP_TYPE);
            } catch (JsonProcessingException ignored) {
                // fall through to raw wrapper
            }
        }
        return Map.of(
            "stdout", stdout.trim(),
            "stderr", stderr.trim(),
            "exitCode", exitCode);
    }

    static String drainStream(InputStream stream, long maxBytes) {
        try {
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            byte[] buffer = new byte[READ_BUFFER_SIZE];
            long totalCaptured = 0;
            int bytesRead;

            while ((bytesRead = stream.read(buffer)) != -1) {
                if (totalCaptured < maxBytes) {
                    int toWrite = (int) Math.min(bytesRead, maxBytes - totalCaptured);
                    captured.write(buffer, 0, toWrite);
                    totalCaptured += toWrite;
                }
                // continue reading to prevent pipe deadlock even after cap
            }

            return captured.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData,
                                              String bindingName) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.name(), capability.name(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId, bindingName);
    }
}
