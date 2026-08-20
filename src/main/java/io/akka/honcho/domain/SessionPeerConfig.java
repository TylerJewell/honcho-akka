package io.akka.honcho.domain;

/** A joined peer's own observation settings — SPEC-001 §2. */
public record SessionPeerConfig(String peerName, boolean observeMe, boolean observeOthers) {}
