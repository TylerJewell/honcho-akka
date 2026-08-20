package io.akka.honcho.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.honcho.domain.SessionEvent;
import io.akka.honcho.domain.SessionState;
import org.junit.jupiter.api.Test;

/** SPEC-001 rules 1-3. */
public class SessionEntityTest {

  private EventSourcedTestKit<SessionState, SessionEvent, SessionEntity> session() {
    return EventSourcedTestKit.of("ws:s1", SessionEntity::new);
  }

  @Test
  public void observerSetIsSenderPlusEveryOtherPeerThatObservesOthers() {
    var kit = session();
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("alice", true, true));
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("bob", true, true));
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("carol", true, false));

    var result = kit.method(SessionEntity::postMessage)
        .invoke(new SessionEntity.PostMessage("alice", "hello there"));
    assertThat(result.getReply().observers()).containsExactlyInAnyOrder("alice", "bob");
  }

  @Test
  public void aSenderWhoDoesNotWantToBeObservedIsInvisibleToEveryObserverNotOnlyThemself() {
    var kit = session();
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("alice", false, true));
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("bob", true, true));

    var result = kit.method(SessionEntity::postMessage)
        .invoke(new SessionEntity.PostMessage("alice", "hi"));
    // Not just "alice" excluded — nobody observes this message at all, including bob, who
    // wants to observe others. observeMe gates the whole observer set (question-log #1,
    // `src/deriver/enqueue.py:340-372`), not only self-observation.
    assertThat(result.getReply().observers()).isEmpty();
  }

  @Test
  public void aPeerWhoHasLeftIsNeverAnObserver() {
    var kit = session();
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("alice", true, true));
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("bob", true, true));
    kit.method(SessionEntity::leave).invoke("bob");

    var result = kit.method(SessionEntity::postMessage)
        .invoke(new SessionEntity.PostMessage("alice", "hi"));
    assertThat(result.getReply().observers()).containsExactly("alice");
  }

  @Test
  public void rejectsEmptyMessage() {
    var kit = session();
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("alice", true, true));
    var result = kit.method(SessionEntity::postMessage)
        .invoke(new SessionEntity.PostMessage("alice", ""));
    assertThat(result.isError()).isTrue();
    assertThat(result.didPersistEvents()).isFalse();
  }

  @Test
  public void retainedMessageHistoryIsBoundedButTheIdCounterIsNot() {
    var kit = session();
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("alice", true, true));
    for (int i = 0; i < 205; i++) {
      kit.method(SessionEntity::postMessage)
          .invoke(new SessionEntity.PostMessage("alice", "msg " + i));
    }
    assertThat(kit.getState().messages()).hasSize(200);
    assertThat(kit.getState().messages().get(0).content()).isEqualTo("msg 5");
    assertThat(kit.getState().nextMessageId()).isEqualTo(206);
  }

  @Test
  public void tokenCountIsContentLengthOverFourRoundedUp() {
    var kit = session();
    kit.method(SessionEntity::join).invoke(new SessionEntity.Join("alice", true, true));
    var result = kit.method(SessionEntity::postMessage)
        .invoke(new SessionEntity.PostMessage("alice", "12345678901")); // 11 chars
    assertThat(result.getReply().message().tokenCount()).isEqualTo(3); // ceil(11/4)
  }
}
