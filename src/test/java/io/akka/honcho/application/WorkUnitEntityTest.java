package io.akka.honcho.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.honcho.domain.PendingMessage;
import io.akka.honcho.domain.WorkUnitEvent;
import io.akka.honcho.domain.WorkUnitState;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 rules 4, 5. */
public class WorkUnitEntityTest {

  private static final Instant T0 = Instant.parse("2026-03-01T12:00:00Z");

  private EventSourcedTestKit<WorkUnitState, WorkUnitEvent, WorkUnitEntity> workUnit() {
    return EventSourcedTestKit.of("ws:s1:alice", WorkUnitEntity::new);
  }

  private PendingMessage msg(int id, int tokens) {
    return new PendingMessage(id, "bob", "x".repeat(tokens), T0, tokens);
  }

  @Test
  public void notReadyBelowThreshold() {
    var kit = workUnit();
    var result = kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(1, 5), List.of("alice", "bob"), 10));
    assertThat(result.getReply().ready()).isFalse();
  }

  @Test
  public void triggersAtThreshold() {
    var kit = workUnit();
    kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(1, 5), List.of("alice", "bob"), 10));
    var result = kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(2, 5), List.of("alice", "bob"), 10));
    assertThat(result.getReply().ready()).isTrue();
  }

  @Test
  public void includesCrossingMessage() {
    var kit = workUnit();
    kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(1, 5), List.of("alice", "bob"), 10));
    var result = kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(2, 8), List.of("alice", "bob"), 10));
    assertThat(result.getReply().batch()).extracting(PendingMessage::messageId)
        .containsExactly(1, 2);
  }

  @Test
  public void everyObserverOfAnObservedPeerSharesOneBatch() {
    var kit = workUnit();
    var withThreeObservers = List.of("alice", "bob", "carol");
    kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(1, 10), withThreeObservers, 10));
    // A different (wrong) observer list on a later message must not change the batch's
    // observer set — it is a property of the batch, taken once from whichever message
    // started it (domain model note).
    var result = kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(2, 1), List.of("dave"), 10));
    assertThat(result.getReply().observers()).containsExactlyInAnyOrder("alice", "bob", "carol");
  }

  @Test
  public void clearingResetsTheBatch() {
    var kit = workUnit();
    kit.method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(msg(1, 10), List.of("alice"), 10));
    kit.method(WorkUnitEntity::clearBatch).invoke();
    assertThat(kit.getState().pending()).isEmpty();
    assertThat(kit.getState().accumulatedTokens()).isZero();
  }
}
