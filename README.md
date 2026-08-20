# honcho-akka

Turns messages posted to a session into a durable, per-observer working understanding of
each peer who sent one.

A port of [plastic-labs/honcho](https://github.com/plastic-labs/honcho) onto **Akka**,
built with **Akka Specify**.

---

## Where it came from

honcho is a memory and reasoning layer that gives a conversational agent a persistent,
evolving model of the people it talks to. It was ported to derive a specification format
precise enough to regenerate a system on a different stack — the port is the vehicle, the
specification is the deliverable.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `honcho-port/`.

---

## honcho → this port

📉 5,224 Python lines → **462 Java lines** (whole project)<br>
📉 2,307 Python lines → **462 Java lines** (like for like)<br>
📁 9 files → **16 files**<br>
⚡ not measured (honcho has no synchronous boundary here — see `bench/REPORT.md` §2) → **24.9 ms** to post a message that stays below the batch threshold<br>
⚡ not measured → **12.9 ms** to read a peer's representation<br>
⚡ not measured → **30.3 ms** to post a message that crosses the threshold — batch, extract, and merge to every observer<br>
🎯 5 questions asked of both, **5 answered the same**

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/honcho-port/bench/REPORT.md).

---

## What it took to build

⏱️ **TBD hours** from the first command to the published repository, **TBD** of them active<br>
💬 **TBD** exchanges with the model<br>
✍️ **TBD** tokens written by the model, **TBD** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **28** tests, plus 3 deliberate breakages to check the tests notice

The record of every decision, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

A **workspace** holds **sessions**; a session's joined **peers** each choose whether they
are observed and whether they observe others. From the specification:

- **A peer who has turned off being observed is invisible to everybody for that
  message, not only to themselves.** honcho builds its whole observer list — self and
  everyone else — behind that one setting, and this port matches it exactly (SPEC-001
  rule 1; this is the one place the first implementation of this port got it wrong, and
  the correction is what `bench/REPORT.md` §1 verifies).
- **Every observer of the same message shares one extraction call.** A message from one
  peer is batched once per `(session, that peer)`, never once per observer, and every
  observer's understanding is updated from that single batch.
- **A batch is claimed once its pending messages cross a token threshold**, and the
  message that crosses it is included in the batch it triggers.
- **The same fact, worded differently, reinforces instead of duplicating.** Content is
  compared after trimming and lowercasing, scoped to the same session; an exact repeat
  bumps a count rather than adding a second row.
- **A representation read is always most-recent-first, and always capped.**

Every rule above is a durable Akka Event Sourced Entity, not an in-memory cache — nothing
here is lost on restart.

---

## Design decisions

**A stand-in for the thinking part.** The step that turns a batch of messages into facts
about a peer is a rule-based sentence splitter, not a call to a language model. Every rule
this port checks is about what happens around that step — batching, sharing, deduplication
— not about how good the extracted facts are, so a benchmark that called a real model would
measure the model instead of the rebuild.

**One entity per shared batch.** Every peer watching the same person share a single record
that holds the pending messages until they are ready. The platform guarantees only one
change to that record happens at a time, so two messages arriving together can never split
into two separate batches by accident.

**Posting, batching, and understanding happen in one request.** honcho spreads this across
a saved job, a separate worker process, and a real model call that can take seconds. This
port does the whole thing before answering the request that posted the message, because
nothing here needs the delay a real model call would add.

**Old history is dropped, not kept forever.** A session's message list and a peer's stored
facts are both capped, with the oldest dropped first once the cap is passed. honcho keeps
everything indefinitely; a service that never forgets grows without bound the longer it
runs.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/honcho-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9017.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

No key for a model provider is needed. Nothing here calls one.

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9017**.

### Try it

```bash
# two peers join a session, both willing to be observed and to observe others
curl -X PUT localhost:9017/workspaces/acme/sessions/standup/peers/alice \
  -H 'Content-Type: application/json' -d '{"observeMe":true,"observeOthers":true}'
curl -X PUT localhost:9017/workspaces/acme/sessions/standup/peers/bob \
  -H 'Content-Type: application/json' -d '{"observeMe":true,"observeOthers":true}'

# a message long enough to cross the batching threshold on its own
curl -X POST localhost:9017/workspaces/acme/sessions/standup/messages \
  -H 'Content-Type: application/json' \
  -d '{"peerName":"alice","content":"I like tea. I work remotely. I like tea. I work remotely."}'

# bob's understanding of alice
curl localhost:9017/workspaces/acme/observers/bob/observed/alice
```

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `akka.javasdk.dev-mode.http-port` | `9017` | Set in `src/main/resources/application.conf`. |
| `honcho.deriver.batch-target-tokens` | `512` | Same default as honcho's `REPRESENTATION_BATCH_WORK_UNIT_TARGET_TOKENS`. Token count is an approximation — see the differences list below. |

---

## Where it differs from honcho

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **The thinking part is a stand-in.** honcho ends every batch in a call to a language
  model; this port answers from a deterministic sentence splitter. Nothing in the
  specification is about what the answer says, and a benchmark that called a model would
  measure the model.
- **Token counts are an approximation, not a real tokenizer.** honcho counts BPE tokens
  with `tiktoken`; this port counts `ceil(content.length() / 4)`. The batching threshold in
  both systems fires at the same configured number of tokens, but that number means a
  different point in the conversation on each side, because the two counting methods
  disagree on ordinary text.
- **A batch is claimed by a token threshold only, not by age as well.** honcho also flushes
  a work unit once its oldest pending message is older than a configured age, so nothing
  waits forever even below the token target. This port implements only the token half — a
  session that goes quiet after one short message waits forever here instead of flushing on
  a timer.
- **Posting, batching, extraction and merging happen inside one HTTP request.** honcho
  spreads the same work across a saved queue row, a separately scheduled worker process,
  and a real model call. This port does the whole thing synchronously, because nothing here
  needs the delay a real model call would add — see `bench/REPORT.md` §2 for what that means
  for the numbers this port can report that honcho cannot.
- **Old history is capped, not kept forever.** A session keeps its most recent 200 messages
  and a representation keeps its most recent 500 observations, oldest dropped first; honcho
  never caps either. This is a review-pass finding (`docs/review-findings.md`), applied
  because an entity's state has no other bound in an event-sourced system, and neither cap
  changes any rule this port checks — every read is already capped by its own
  caller-supplied limit.
- **Semantic (embedding-similarity) deduplication does not exist here.** honcho runs a
  second dedup pass over vector similarity, after the exact-match pass this port
  implements; that pass, and the vector store it depends on, are out of scope for this
  slice (SPEC-001 §1).
- **Not checked: what a caller sees under load from many sessions at once.** Both systems
  were measured one session at a time.

---

## Licence

honcho is AGPL-3.0, © Plastic Labs. This port reimplements the behaviour without copied
source; see [`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md) for what that does and does not
settle about this repository staying private.
