package io.akka.honcho.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Delete;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.annotations.http.Put;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.AbstractHttpEndpoint;
import com.typesafe.config.Config;
import io.akka.honcho.application.DeriverPipeline;
import io.akka.honcho.application.SessionEntity;
import io.akka.honcho.domain.Observation;
import java.util.List;

/**
 * The HTTP surface for the deriver slice — join/leave a session, post a message, and read a
 * peer's representation as seen by one observer.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/workspaces")
public class SessionEndpoint extends AbstractHttpEndpoint {

  private final ComponentClient componentClient;
  private final DeriverPipeline pipeline;

  public SessionEndpoint(ComponentClient componentClient, Config config) {
    this.componentClient = componentClient;
    this.pipeline = new DeriverPipeline(componentClient, config);
  }

  public record JoinBody(Boolean observeMe, Boolean observeOthers) {}

  @Put("/{workspace}/sessions/{session}/peers/{peer}")
  public void join(String workspace, String session, String peer, JoinBody body) {
    componentClient.forEventSourcedEntity(workspace + ":" + session)
        .method(SessionEntity::join)
        .invoke(new SessionEntity.Join(peer, body.observeMe(), body.observeOthers()));
  }

  @Delete("/{workspace}/sessions/{session}/peers/{peer}")
  public void leave(String workspace, String session, String peer) {
    componentClient.forEventSourcedEntity(workspace + ":" + session)
        .method(SessionEntity::leave)
        .invoke(peer);
  }

  public record PostMessageBody(String peerName, String content) {}

  @Post("/{workspace}/sessions/{session}/messages")
  public DeriverPipeline.Posted postMessage(String workspace, String session,
      PostMessageBody body) {
    return pipeline.postMessage(workspace, session, body.peerName(), body.content());
  }

  @Get("/{workspace}/observers/{observer}/observed/{observed}")
  public List<Observation> representation(String workspace, String observer, String observed) {
    var limit = requestContext().queryParams().getInteger("limit").orElse(25);
    return pipeline.recent(workspace, observer, observed, limit);
  }
}
