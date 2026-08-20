package io.akka.honcho.domain;

import akka.javasdk.annotations.TypeName;
import java.time.Instant;

/** Everything that can happen to a session. */
public sealed interface SessionEvent {

  @TypeName("peer-joined")
  record PeerJoined(String peerName, boolean observeMe, boolean observeOthers)
      implements SessionEvent {}

  @TypeName("peer-left")
  record PeerLeft(String peerName) implements SessionEvent {}

  @TypeName("message-posted")
  record MessagePosted(int id, String peerName, String content, Instant createdAt, int tokenCount)
      implements SessionEvent {}
}
