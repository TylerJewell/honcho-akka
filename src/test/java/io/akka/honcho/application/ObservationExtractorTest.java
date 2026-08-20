package io.akka.honcho.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.akka.honcho.domain.PendingMessage;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 rule 6 — this pipeline's only output is explicit facts about the observed peer. */
public class ObservationExtractorTest {

  private static final Instant T0 = Instant.parse("2026-03-01T12:00:00Z");

  private PendingMessage from(String peer, String content) {
    return new PendingMessage(1, peer, content, T0, 4);
  }

  @Test
  public void everyFactNamesTheObservedPeerAndCarriesNoOtherKind() {
    var facts = ObservationExtractor.extract(
        List.of(from("alice", "I like tea.")), "alice");
    assertThat(facts).allSatisfy(f -> assertThat(f).startsWith("alice "));
  }

  @Test
  public void aFirstPersonStatementFromTheObservedPeerIsRewrittenInThirdPerson() {
    var facts = ObservationExtractor.extract(
        List.of(from("alice", "I like tea.")), "alice");
    assertThat(facts).containsExactly("alice like tea.");
  }

  @Test
  public void aThirdPersonMentionByAnotherSenderIsKeptAsIs() {
    var facts = ObservationExtractor.extract(
        List.of(from("bob", "Alice works remotely.")), "alice");
    assertThat(facts).containsExactly("alice alice works remotely.");
  }

  @Test
  public void aSentenceThatNeitherMentionsTheObservedPeerNorComesFromThemProducesNoFact() {
    var facts = ObservationExtractor.extract(
        List.of(from("bob", "The weather is nice today.")), "alice");
    assertThat(facts).isEmpty();
  }
}
