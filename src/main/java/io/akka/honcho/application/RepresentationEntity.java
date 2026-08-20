package io.akka.honcho.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import io.akka.honcho.domain.Observation;
import io.akka.honcho.domain.RepresentationEvent;
import io.akka.honcho.domain.RepresentationState;
import java.time.Instant;
import java.util.List;

/**
 * One observer's stored facts about one observed peer — SPEC-001 rules 7-12, 14.
 *
 * <p>The entity id is {@code workspace:observer:observed}. All the rules live in
 * {@link RepresentationState}; this class decides only what to persist and what to answer.
 */
@Component(id = "representation")
public class RepresentationEntity extends EventSourcedEntity<RepresentationState, RepresentationEvent> {

  @Override
  public RepresentationState emptyState() {
    return RepresentationState.empty();
  }

  public record Merge(List<String> facts, List<Integer> messageIds, String sessionName) {}

  public Effect<Done> merge(Merge command) {
    var answer = currentState().merge(command.facts(), command.messageIds(),
        command.sessionName(), Instant.now());
    return switch (answer) {
      case RepresentationState.Answer.Refused r -> effects().error(r.reason());
      case RepresentationState.Answer.Ok ok -> ok.events().isEmpty()
          ? effects().reply(Done.getInstance())
          : effects().persistAll(ok.events()).thenReply(state -> Done.getInstance());
    };
  }

  public record Recent(int limit) {}

  public ReadOnlyEffect<List<Observation>> recent(Recent command) {
    return effects().reply(currentState().recent(command.limit()));
  }

  @Override
  public RepresentationState applyEvent(RepresentationEvent event) {
    return currentState().apply(event);
  }
}
