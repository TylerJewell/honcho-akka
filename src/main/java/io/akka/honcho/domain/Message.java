package io.akka.honcho.domain;

import java.time.Instant;

/** One message posted to a session — SPEC-001 §2. */
public record Message(int id, String peerName, String content, Instant createdAt, int tokenCount) {}
