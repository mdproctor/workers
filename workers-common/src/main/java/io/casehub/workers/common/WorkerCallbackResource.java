package io.casehub.workers.common;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import io.casehub.platform.api.mcp.HandWrittenEndpoint;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Optional;

@HandWrittenEndpoint("async worker completion callback")
@Path("/workers/complete")
@ApplicationScoped
public class WorkerCallbackResource {

    @Inject
    AsyncWorkerCompletionRegistry registry;

    @Inject
    WorkflowCompletionPublisher completionPublisher;

    @Inject
    Event<FaultCallbackEvent> faultCallbackEvents;

    @POST
    @Path("/{dispatchId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response complete(@PathParam("dispatchId") String dispatchId,
                             @HeaderParam("X-Casehub-Callback-Token") String token,
                             WorkerCompletionPayload payload) {
        Optional<PendingCompletion> maybePending = registry.complete(dispatchId);
        if (maybePending.isEmpty()) {
            return Response.status(404).build();
        }

        PendingCompletion pending = maybePending.get();

        if (!MessageDigest.isEqual(
                pending.callbackToken().getBytes(), token != null ? token.getBytes() : new byte[0])) {
            java.time.Duration remaining = java.time.Duration.between(java.time.Instant.now(), pending.expiresAt());
            if (!remaining.isNegative()) {
                registry.register(pending.workerType(), pending.faultAddress(),
                    pending.correlationContext(),
                    pending.capability(), pending.eventLogId(),
                    remaining, pending.provisionerMeta());
            }
            return Response.status(401).build();
        }

        if (payload.faulted()) {
            faultCallbackEvents.fireAsync(new FaultCallbackEvent(
                pending,
                payload.errorMessage() != null ? new RuntimeException(payload.errorMessage()) : null));
        } else {
            completionPublisher.complete(pending.correlationContext(),
                payload.output() != null ? payload.output() : Map.of());
        }
        return Response.ok().build();
    }
}
