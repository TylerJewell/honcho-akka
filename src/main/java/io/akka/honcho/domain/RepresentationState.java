package io.akka.honcho.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * One observer's stored facts about one observed peer — SPEC-001 §2, rules 7-12, 14.
 */
public record RepresentationState(List<Observation> observations) {

  /** Retained observations are bounded so a peer observed for a long time cannot grow this
   * entity's state without limit. honcho itself does not cap stored documents; this port
   * does, and the oldest observation is dropped first (SPEC-001 has no rule against it,
   * since every read is already capped by its own caller-supplied limit — rule 14). */
  static final int MAX_OBSERVATIONS = 500;

  public static RepresentationState empty() {
    return new RepresentationState(List.of());
  }

  public sealed interface Answer {
    record Refused(String reason) implements Answer {}

    record Ok(List<RepresentationEvent> events) implements Answer {}
  }

  private static String normalize(String content) {
    return content.strip().toLowerCase(Locale.ROOT);
  }

  /**
   * Rule 9 refuses a blank session. Rule 11 collapses an exact duplicate that appears
   * twice within the same batch before either copy is compared to storage. Rules 7, 8, 10
   * normalize, scope to {@code sessionName}, and reinforce rather than duplicate an
   * existing exact match.
   */
  public Answer merge(List<String> factTexts, List<Integer> messageIds, String sessionName,
      Instant now) {
    if (sessionName == null || sessionName.isBlank()) {
      return new Answer.Refused("an observation must carry a session name");
    }
    var events = new ArrayList<RepresentationEvent>();
    var seenInBatch = new HashSet<String>();
    List<Observation> working = new ArrayList<>(observations);
    for (var raw : factTexts) {
      if (raw == null || raw.isBlank()) {
        continue;
      }
      var key = normalize(raw);
      if (!seenInBatch.add(key)) {
        continue;
      }
      var existing = working.stream()
          .filter(o -> o.sessionName().equals(sessionName) && normalize(o.content()).equals(key))
          .findFirst();
      if (existing.isPresent()) {
        var event = new RepresentationEvent.Reinforced(existing.get().id(), now);
        events.add(event);
        working = withReinforced(working, event);
      } else {
        var observation =
            new Observation(UUID.randomUUID().toString(), raw, sessionName, messageIds, now, 1);
        events.add(new RepresentationEvent.Added(observation));
        working.add(observation);
      }
    }
    return new Answer.Ok(List.copyOf(events));
  }

  private static List<Observation> withReinforced(List<Observation> obs,
      RepresentationEvent.Reinforced event) {
    return obs.stream()
        .map(o -> o.id().equals(event.observationId())
            ? new Observation(o.id(), o.content(), o.sessionName(), o.messageIds(),
                o.createdAt(), o.timesDerived() + 1)
            : o)
        .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
  }

  public RepresentationState apply(RepresentationEvent event) {
    return switch (event) {
      case RepresentationEvent.Added e -> {
        var updated = new ArrayList<>(observations);
        updated.add(e.observation());
        if (updated.size() > MAX_OBSERVATIONS) {
          updated.sort(Comparator.comparing(Observation::createdAt));
          updated.remove(0);
        }
        yield new RepresentationState(List.copyOf(updated));
      }
      case RepresentationEvent.Reinforced e -> new RepresentationState(withReinforced(observations, e));
    };
  }

  /** Rule 14 — most-recent-first, capped at the caller's limit. */
  public List<Observation> recent(int limit) {
    return observations.stream()
        .sorted(Comparator.comparing(Observation::createdAt).reversed())
        .limit(limit)
        .toList();
  }
}
