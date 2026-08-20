package io.akka.honcho.application;

import akka.javasdk.client.ComponentClient;
import com.typesafe.config.Config;
import io.akka.honcho.domain.Message;
import io.akka.honcho.domain.Observation;
import io.akka.honcho.domain.PendingMessage;
import java.util.List;

/**
 * Message posted → enqueued → (maybe) extracted → merged, start to finish — SPEC-001
 * rules 1, 2, 4-6, 13.
 *
 * <p>A plain orchestrating class, not an entity: question-log #8 established that one Akka
 * component calls another as an ordinary, non-blocking-within-the-caller call from code like
 * this, the same shape {@code intentkit-akka}'s {@code TaskRunner} already uses to drive
 * several entities from one place. Nothing here needs locking of its own — the entities it
 * calls each serialize on their own id (rule 13).
 */
public final class DeriverPipeline {

  private final ComponentClient componentClient;
  private final int thresholdTokens;

  public DeriverPipeline(ComponentClient componentClient, Config config) {
    this.componentClient = componentClient;
    this.thresholdTokens = config.getInt("honcho.deriver.batch-target-tokens");
  }

  public record Posted(Message message, List<String> observers) {}

  /**
   * Appends the message to the session, then — one call per observed peer whose batch is
   * ready — extracts facts and merges them into every observer's representation (rule 4:
   * one shared batch and one extraction call per {@code (workspace, session, observed)}
   * triple, never one per observer).
   */
  public Posted postMessage(String workspace, String session, String peerName, String content) {
    var sessionId = workspace + ":" + session;
    var result = componentClient.forEventSourcedEntity(sessionId)
        .method(SessionEntity::postMessage)
        .invoke(new SessionEntity.PostMessage(peerName, content));

    var pending = new PendingMessage(result.message().id(), result.message().peerName(),
        result.message().content(), result.message().createdAt(), result.message().tokenCount());
    var workUnitId = workspace + ":" + session + ":" + peerName;
    var added = componentClient.forEventSourcedEntity(workUnitId)
        .method(WorkUnitEntity::addMessage)
        .invoke(new WorkUnitEntity.AddMessage(pending, result.observers(), thresholdTokens));

    if (added.ready()) {
      var facts = ObservationExtractor.extract(added.batch(), peerName);
      var messageIds = added.batch().stream().map(PendingMessage::messageId).toList();
      for (var observer : added.observers()) {
        var representationId = workspace + ":" + observer + ":" + peerName;
        componentClient.forEventSourcedEntity(representationId)
            .method(RepresentationEntity::merge)
            .invoke(new RepresentationEntity.Merge(facts, messageIds, session));
      }
      componentClient.forEventSourcedEntity(workUnitId).method(WorkUnitEntity::clearBatch)
          .invoke();
    }

    return new Posted(result.message(), result.observers());
  }

  public List<Observation> recent(String workspace, String observer, String observed, int limit) {
    var representationId = workspace + ":" + observer + ":" + observed;
    return componentClient.forEventSourcedEntity(representationId)
        .method(RepresentationEntity::recent)
        .invoke(new RepresentationEntity.Recent(limit));
  }
}
