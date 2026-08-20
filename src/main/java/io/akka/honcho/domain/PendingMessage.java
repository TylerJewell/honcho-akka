package io.akka.honcho.domain;

import java.time.Instant;

/** One message waiting in a work unit's batch — SPEC-001 §2. */
public record PendingMessage(
    int messageId, String peerName, String content, Instant createdAt, int tokenCount) {}
