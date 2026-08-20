package io.akka.honcho.application;

import io.akka.honcho.domain.PendingMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A deterministic, rule-based stand-in for honcho's extraction LLM call — SPEC-001 §4
 * decision 3. Splits each pending message into sentences and keeps the ones that are either
 * a first-person statement from the observed peer or a third-person statement naming them,
 * turning each into a {@code "<observed> <claim>"} fact. Exercises every rule in §3 that
 * depends on what a batch produces — batching, sharing, dedup, reinforcement — without
 * claiming to match the source's actual linguistic judgment.
 */
public final class ObservationExtractor {

  private ObservationExtractor() {}

  public static List<String> extract(List<PendingMessage> batch, String observed) {
    var facts = new ArrayList<String>();
    for (var message : batch) {
      for (var sentence : message.content().split("(?<=[.!?])\\s+")) {
        var trimmed = sentence.strip();
        if (trimmed.isEmpty()) {
          continue;
        }
        String claim;
        if (message.peerName().equals(observed)) {
          claim = firstPersonToThirdPerson(trimmed);
        } else if (mentions(trimmed, observed)) {
          claim = trimmed.toLowerCase(Locale.ROOT);
        } else {
          continue;
        }
        facts.add(observed + " " + claim);
      }
    }
    return facts;
  }

  private static boolean mentions(String sentence, String name) {
    return sentence.toLowerCase(Locale.ROOT).contains(name.toLowerCase(Locale.ROOT));
  }

  private static String firstPersonToThirdPerson(String sentence) {
    var text = sentence.strip();
    if (text.regionMatches(true, 0, "i am ", 0, 5)) {
      text = "is " + text.substring(5);
    } else if (text.regionMatches(true, 0, "i'm ", 0, 4)) {
      text = "is " + text.substring(4);
    } else if (text.regionMatches(true, 0, "i ", 0, 2)) {
      text = text.substring(2);
    }
    return text.strip().toLowerCase(Locale.ROOT);
  }
}
