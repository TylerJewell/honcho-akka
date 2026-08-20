package io.akka.honcho.domain;

import akka.javasdk.annotations.TypeName;
import java.util.List;

/** Everything that can happen to one work unit's pending batch. */
public sealed interface WorkUnitEvent {

  @TypeName("message-queued")
  record MessageQueued(PendingMessage message, List<String> observers) implements WorkUnitEvent {}

  @TypeName("batch-cleared")
  record BatchCleared() implements WorkUnitEvent {}
}
