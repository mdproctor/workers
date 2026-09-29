package io.casehub.workers.spring.integration;

import io.casehub.api.model.event.CaseHubEventType;
import io.casehub.api.model.event.EventStreamType;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointQuery;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.path.Path;
import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@TestConfiguration
public class TestStubConfiguration {

    @Bean
    public EventLogRepository eventLogRepository() {
        return new EventLogRepository() {
            @Override public void append(EventLog e, String t) {}
            @Override public Long appendAndReturnId(EventLog e, String t) { return 0L; }
            @Override public EventLog findById(Long id, String t) { return null; }
            @Override public List<EventLog> findSchedulingEvents(UUID c, String w, Instant a, String t) { return List.of(); }
            @Override public List<EventLog> findByCaseAndTypes(UUID c, Collection<CaseHubEventType> types, String t) { return List.of(); }
            @Override public List<EventLog> findByCaseAndWorkerAndType(UUID c, String w, CaseHubEventType type, String t) { return List.of(); }
            @Override public List<EventLog> findByWorkerAndType(String w, CaseHubEventType type, String t) { return List.of(); }
            @Override public List<EventLog> findByCaseWithFilters(UUID c, Collection<CaseHubEventType> et, Collection<EventStreamType> st, String t) { return List.of(); }
        };
    }

    @Bean
    @org.springframework.context.annotation.Primary
    public WorkerExecutionManager workerExecutionManager() {
        return new WorkerExecutionManager() {
            @Override public boolean supports(String cap, String t) { return false; }
            @Override public void submit(Long eid, CaseInstance ci, Worker w, Capability c, Map<String, Object> d) {}
            @Override public int getActiveWorkCount(String wid) { return 0; }
        };
    }

    @Bean
    public EndpointRegistry endpointRegistry() {
        return new EndpointRegistry() {
            @Override public void register(EndpointDescriptor d) {}
            @Override public Optional<EndpointDescriptor> resolve(Path p, String t) { return Optional.empty(); }
            @Override public List<EndpointDescriptor> discover(EndpointQuery q) { return List.of(); }
            @Override public void deregister(Path p, String t) {}
        };
    }
}
