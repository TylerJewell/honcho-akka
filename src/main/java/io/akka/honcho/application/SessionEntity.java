package io.akka.honcho.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.honcho.domain.Message;
import io.akka.honcho.domain.SessionEvent;
import io.akka.honcho.domain.SessionState;
import java.time.Instant;
import java.util.List;

/**
 * One session's joined peers and message history — SPEC-001 rules 1-3.
 *
 * <p>The entity id is {@code workspace:session}. All the rules live in {@link SessionState};
 * this class decides only what to persist and what to answer.
 */
@Component(id = "session")
public class SessionEntity extends EventSourcedEntity<SessionState, SessionEvent> {

  @Override
  public SessionState emptyState() {
    return SessionState.empty();
  }

  public record Join(String peerName, Boolean observeMe, Boolean observeOthers) {}

  /** honcho's own default for a joined peer's settings is {@code true} for both. */
  public Effect<Done> join(Join command) {
    boolean observeMe = command.observeMe() == null || command.observeMe();
    boolean observeOthers = command.observeOthers() == null || command.observeOthers();
    return effects()
        .persist(new SessionEvent.PeerJoined(command.peerName(), observeMe, observeOthers))
        .thenReply(state -> Done.getInstance());
  }

  public Effect<Done> leave(String peerName) {
    return effects().persist(new SessionEvent.PeerLeft(peerName))
        .thenReply(state -> Done.getInstance());
  }

  public record PostMessage(String peerName, String content) {}

  public record PostMessageResult(Message message, List<String> observers) {}

  /** Rule 3 rejects empty content before it is appended or counted toward any batch. */
  public Effect<PostMessageResult> postMessage(PostMessage command) {
    if (command.content() == null || command.content().isEmpty()) {
      return effects().error("message content must not be empty");
    }
    if (!currentState().peers().containsKey(command.peerName())) {
      return effects().error("peer " + command.peerName() + " has not joined this session");
    }
    int tokenCount = (int) Math.ceil(command.content().length() / 4.0);
    var createdAt = Instant.now();
    return effects()
        .persist(new SessionEvent.MessagePosted(currentState().nextMessageId(),
            command.peerName(), command.content(), createdAt, tokenCount))
        .thenReply(state -> new PostMessageResult(
            state.messages().get(state.messages().size() - 1),
            state.observers(command.peerName())));
  }

  public ReadOnlyEffect<SessionState> get() {
    return effects().reply(currentState());
  }

  @Override
  public SessionState applyEvent(SessionEvent event) {
    return currentState().apply(event);
  }
}
