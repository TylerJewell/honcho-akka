package io.akka.honcho.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import io.akka.honcho.domain.PendingMessage;
import io.akka.honcho.domain.WorkUnitEvent;
import io.akka.honcho.domain.WorkUnitState;
import java.util.List;

/**
 * One {@code (workspace, session, observed)} work unit — SPEC-001 rules 4, 5, 13.
 *
 * <p>The entity id is {@code workspace:session:observed}, shared by every observer of that
 * peer (question-log #1), so the Akka runtime's per-entity-id serialization is what keeps
 * concurrent triggers on the same triple from ever producing two extraction calls (rule 13).
 */
@Component(id = "work-unit")
public class WorkUnitEntity extends EventSourcedEntity<WorkUnitState, WorkUnitEvent> {

  @Override
  public WorkUnitState emptyState() {
    return WorkUnitState.empty();
  }

  public record AddMessage(PendingMessage message, List<String> observers, int thresholdTokens) {}

  public record AddResult(boolean ready, List<PendingMessage> batch, List<String> observers) {}

  public Effect<AddResult> addMessage(AddMessage command) {
    return effects()
        .persist(new WorkUnitEvent.MessageQueued(command.message(), command.observers()))
        .thenReply(state -> state.ready(command.thresholdTokens())
            ? new AddResult(true, state.pending(), state.observers())
            : new AddResult(false, List.of(), List.of()));
  }

  public Effect<Done> clearBatch() {
    return effects().persist(new WorkUnitEvent.BatchCleared())
        .thenReply(state -> Done.getInstance());
  }

  public ReadOnlyEffect<WorkUnitState> get() {
    return effects().reply(currentState());
  }

  @Override
  public WorkUnitState applyEvent(WorkUnitEvent event) {
    return currentState().apply(event);
  }
}
