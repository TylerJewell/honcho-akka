package io.akka.honcho.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A session's joined peers and posted messages — SPEC-001 §2, rules 1-3.
 *
 * <p>A peer who has left is removed from {@link #peers()} entirely, so {@link #observers}
 * excludes them by construction rather than by a separate check (rule 1).
 */
public record SessionState(Map<String, SessionPeerConfig> peers, List<Message> messages,
    int nextMessageId) {

  /** Retained history is bounded so an old, long-lived session cannot grow this entity's
   * state without limit — the id counter, not this list, is what message ordering depends
   * on, so trimming the oldest entries changes nothing rules 1-3 check. */
  static final int MAX_MESSAGES = 200;

  public static SessionState empty() {
    return new SessionState(Map.of(), List.of(), 1);
  }

  public SessionState apply(SessionEvent event) {
    return switch (event) {
      case SessionEvent.PeerJoined e -> {
        var updated = new LinkedHashMap<>(peers);
        updated.put(e.peerName(),
            new SessionPeerConfig(e.peerName(), e.observeMe(), e.observeOthers()));
        yield new SessionState(updated, messages, nextMessageId);
      }
      case SessionEvent.PeerLeft e -> {
        var updated = new LinkedHashMap<>(peers);
        updated.remove(e.peerName());
        yield new SessionState(updated, messages, nextMessageId);
      }
      case SessionEvent.MessagePosted e -> {
        var updated = new ArrayList<>(messages);
        updated.add(new Message(e.id(), e.peerName(), e.content(), e.createdAt(), e.tokenCount()));
        while (updated.size() > MAX_MESSAGES) {
          updated.remove(0);
        }
        yield new SessionState(peers, List.copyOf(updated), nextMessageId + 1);
      }
    };
  }

  /** Rule 1 — {@code observeMe} is whether the sender is observed at all, not only whether
   * they observe themselves: honcho's own {@code generate_queue_records} builds the whole
   * observer list, self and everyone else, inside its {@code if should_observe:} branch
   * (`src/deriver/enqueue.py:340-372`), so a sender who has turned it off is invisible to a
   * peer who wants to observe others just as much as to themselves. Rule 2 is the self
   * inclusion once {@code observeMe} holds. */
  public List<String> observers(String sender) {
    var senderConfig = peers.get(sender);
    if (senderConfig == null || !senderConfig.observeMe()) {
      return List.of();
    }
    var result = new ArrayList<String>();
    result.add(sender);
    for (var peer : peers.values()) {
      if (!peer.peerName().equals(sender) && peer.observeOthers()) {
        result.add(peer.peerName());
      }
    }
    return List.copyOf(result);
  }
}
