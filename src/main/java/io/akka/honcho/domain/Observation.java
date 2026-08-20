package io.akka.honcho.domain;

import java.time.Instant;
import java.util.List;

/** One stored fact about a peer, as seen by one observer — SPEC-001 §2. */
public record Observation(String id, String content, String sessionName,
    List<Integer> messageIds, Instant createdAt, int timesDerived) {}
