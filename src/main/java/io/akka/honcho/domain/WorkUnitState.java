package io.akka.honcho.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * One {@code (workspace, session, observed)} work unit's pending batch — SPEC-001 §2,
 * rules 4, 5.
 *
 * <p>{@code observers} is carried once per batch, taken from whichever message started it,
 * rather than recomputed per message — the domain model's own note that the observer set is
 * a property of the batch, not of each message in it.
 */
public record WorkUnitState(List<PendingMessage> pending, int accumulatedTokens,
    List<String> observers) {

  public static WorkUnitState empty() {
    return new WorkUnitState(List.of(), 0, List.of());
  }

  public WorkUnitState apply(WorkUnitEvent event) {
    return switch (event) {
      case WorkUnitEvent.MessageQueued e -> {
        var updated = new ArrayList<>(pending);
        updated.add(e.message());
        var batchObservers = pending.isEmpty() ? e.observers() : observers;
        yield new WorkUnitState(List.copyOf(updated),
            accumulatedTokens + e.message().tokenCount(), batchObservers);
      }
      case WorkUnitEvent.BatchCleared e -> WorkUnitState.empty();
    };
  }

  /** Rule 5 — the message that crosses the threshold is included in the batch it triggers. */
  public boolean ready(int thresholdTokens) {
    return accumulatedTokens >= thresholdTokens;
  }
}
