package io.akka.honcho.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.honcho.domain.RepresentationEvent;
import io.akka.honcho.domain.RepresentationState;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 rules 7-12, 14. */
public class RepresentationEntityTest {

  private EventSourcedTestKit<RepresentationState, RepresentationEvent, RepresentationEntity> rep() {
    return EventSourcedTestKit.of("ws:bob:alice", RepresentationEntity::new);
  }

  @Test
  public void nonDuplicateAlwaysStored() {
    var kit = rep();
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(
            List.of("alice likes tea", "alice works remotely"), List.of(1), "sess-1"));
    assertThat(kit.getState().observations()).hasSize(2);
  }

  @Test
  public void dedupNormalizesWhitespaceAndCase() {
    var kit = rep();
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("Alice likes tea"), List.of(1), "sess-1"));
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("  alice LIKES tea  "), List.of(2), "sess-1"));
    assertThat(kit.getState().observations()).hasSize(1);
    assertThat(kit.getState().observations().get(0).timesDerived()).isEqualTo(2);
  }

  @Test
  public void doesNotDedupAcrossSessions() {
    var kit = rep();
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("alice likes tea"), List.of(1), "sess-1"));
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("alice likes tea"), List.of(2), "sess-2"));
    assertThat(kit.getState().observations()).hasSize(2);
  }

  @Test
  public void reinforcementBumpsTimesDerivedOnly() {
    var kit = rep();
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("alice likes tea"), List.of(1), "sess-1"));
    var first = kit.getState().observations().get(0);

    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("alice likes tea"), List.of(2), "sess-1"));
    var second = kit.getState().observations().get(0);

    assertThat(second.id()).isEqualTo(first.id());
    assertThat(second.content()).isEqualTo(first.content());
    assertThat(second.createdAt()).isEqualTo(first.createdAt());
    assertThat(second.timesDerived()).isEqualTo(2);
  }

  @Test
  public void collapsesInBatchDuplicate() {
    var kit = rep();
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(
            List.of("alice likes tea", "Alice Likes Tea"), List.of(1), "sess-1"));
    assertThat(kit.getState().observations()).hasSize(1);
    assertThat(kit.getState().observations().get(0).timesDerived()).isEqualTo(1);
  }

  @Test
  public void refusesBlankSession() {
    var kit = rep();
    var result = kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("alice likes tea"), List.of(1), "  "));
    assertThat(result.isError()).isTrue();
    assertThat(result.didPersistEvents()).isFalse();
  }

  @Test
  public void readOrdersRecentFirst() throws InterruptedException {
    var kit = rep();
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("alice likes tea"), List.of(1), "sess-1"));
    Thread.sleep(5);
    kit.method(RepresentationEntity::merge)
        .invoke(new RepresentationEntity.Merge(List.of("alice likes coffee"), List.of(2), "sess-1"));

    var recent = kit.method(RepresentationEntity::recent)
        .invoke(new RepresentationEntity.Recent(10)).getReply();
    assertThat(recent).extracting(o -> o.content()).containsExactly(
        "alice likes coffee", "alice likes tea");
  }

  @Test
  public void retainedObservationsAreBoundedOldestFirst() {
    var kit = rep();
    for (int i = 0; i < 505; i++) {
      kit.method(RepresentationEntity::merge)
          .invoke(new RepresentationEntity.Merge(List.of("alice fact " + i), List.of(i), "sess-1"));
    }
    assertThat(kit.getState().observations()).hasSize(500);
    assertThat(kit.getState().observations().stream().map(o -> o.content()))
        .doesNotContain("alice fact 0", "alice fact 1", "alice fact 2", "alice fact 3",
            "alice fact 4")
        .contains("alice fact 504");
  }

  @Test
  public void readRespectsLimit() {
    var kit = rep();
    kit.method(RepresentationEntity::merge).invoke(new RepresentationEntity.Merge(
        List.of("alice likes tea", "alice likes coffee", "alice likes cake"),
        List.of(1), "sess-1"));

    var recent = kit.method(RepresentationEntity::recent)
        .invoke(new RepresentationEntity.Recent(2)).getReply();
    assertThat(recent).hasSize(2);
  }
}
