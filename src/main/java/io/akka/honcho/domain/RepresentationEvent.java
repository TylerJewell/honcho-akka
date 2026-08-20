package io.akka.honcho.domain;

import akka.javasdk.annotations.TypeName;
import java.time.Instant;

/** Everything that can happen to one observer's representation of one observed peer. */
public sealed interface RepresentationEvent {

  @TypeName("observation-added")
  record Added(Observation observation) implements RepresentationEvent {}

  @TypeName("observation-reinforced")
  record Reinforced(String observationId, Instant at) implements RepresentationEvent {}
}
