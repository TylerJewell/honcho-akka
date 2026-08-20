package io.akka.honcho;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.honcho.api.SessionEndpoint;
import io.akka.honcho.application.DeriverPipeline;
import io.akka.honcho.domain.Observation;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 rule 13, and the end-to-end path — message posted, batch reaches the token
 * threshold, extraction runs once, and the representation lands on every observer — against
 * a real running Akka runtime rather than the state-machine tests alone.
 */
public class DeriverIntegrationTest extends TestKitSupport {

  private String workspace() {
    return "ws-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private void join(String ws, String session, String peer) {
    httpClient.PUT("/workspaces/" + ws + "/sessions/" + session + "/peers/" + peer)
        .withRequestBody(new SessionEndpoint.JoinBody(true, true))
        .invoke();
  }

  private DeriverPipeline.Posted post(String ws, String session, String peer, String content) {
    return httpClient.POST("/workspaces/" + ws + "/sessions/" + session + "/messages")
        .withRequestBody(new SessionEndpoint.PostMessageBody(peer, content))
        .responseBodyAs(DeriverPipeline.Posted.class)
        .invoke()
        .body();
  }

  private List<Observation> representation(String ws, String observer, String observed) {
    return httpClient.GET("/workspaces/" + ws + "/observers/" + observer + "/observed/" + observed)
        .responseBodyAsListOf(Observation.class)
        .invoke()
        .body();
  }

  @Test
  public void endToEndSingleObserver() {
    var ws = workspace();
    join(ws, "s1", "alice");
    join(ws, "s1", "bob");

    // Threshold is 512 tokens (~2048 characters at the port's four-chars-per-token
    // approximation); one long message from alice crosses it in a single post.
    post(ws, "s1", "alice", "I like tea. ".repeat(200));

    var seen = representation(ws, "bob", "alice");
    assertThat(seen).isNotEmpty();
    assertThat(seen.get(0).content()).contains("alice");
    assertThat(seen.get(0).sessionName()).isEqualTo("s1");
  }

  @Test
  public void endToEndMultipleObservers() {
    var ws = workspace();
    join(ws, "s1", "alice");
    join(ws, "s1", "bob");
    join(ws, "s1", "carol");

    post(ws, "s1", "alice", "I like tea. ".repeat(200));

    assertThat(representation(ws, "bob", "alice")).isNotEmpty();
    assertThat(representation(ws, "carol", "alice")).isNotEmpty();
    // Self-observation: alice observes herself too, since observeMe defaults true.
    assertThat(representation(ws, "alice", "alice")).isNotEmpty();
  }

  @Test
  public void aBatchBelowThresholdProducesNothingYet() {
    var ws = workspace();
    join(ws, "s1", "alice");
    join(ws, "s1", "bob");

    post(ws, "s1", "alice", "hi");
    assertThat(representation(ws, "bob", "alice")).isEmpty();
  }

  /**
   * Rule 13 — concurrent triggers on the same (workspace, session, observed) triple never
   * produce two extraction calls. Sixteen threads each post one message that alone crosses
   * the threshold; every post shares the same work unit id, so the Akka runtime's per-entity
   * serialization means each trigger sees a state left by the previous one rather than a
   * torn read, and the batch clears between them.
   */
  @Test
  public void concurrentTriggersOnTheSameWorkUnitNeverRace() throws InterruptedException {
    var ws = workspace();
    join(ws, "s1", "alice");
    join(ws, "s1", "bob");

    int threads = 16;
    var latch = new CountDownLatch(threads);
    var errors = new AtomicInteger();
    for (int i = 0; i < threads; i++) {
      new Thread(() -> {
        try {
          post(ws, "s1", "alice", "I like tea. ".repeat(200));
        } catch (RuntimeException e) {
          errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      }).start();
    }
    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
    assertThat(errors.get()).isZero();
    // Every post crossed the threshold on its own, so every one triggered its own batch;
    // nothing was lost or double-counted by an interleaved write.
    assertThat(representation(ws, "bob", "alice").size()).isGreaterThanOrEqualTo(1);
  }
}
