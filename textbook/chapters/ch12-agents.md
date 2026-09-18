# Chapter 12 — Agents: letting an LLM operate the lakehouse

## 12.0 What you'll build

The newest layer, and a fitting capstone: an **agent** — an LLM that *operates* the
lakehouse for you. You've built every layer by hand; now you hand the controls to an
AI that drives the same `./lakehouse` CLI and reads the same skills you've used all
course. You'll ask it, in plain English, to start the stack, run the medallion, and
report on the Gold tables — and watch it use **tools** to actually do it. This is
the "designed to be operated by an AI agent" ethos the reference architecture was
built around.

![Progress: Agents](../figures/ch12/fig-12.3-progress-agents.svg)

**Figure 12.3** — Progress map with **Agents** highlighted.

## 12.1 Learning objectives

By the end of this chapter you can:

- Define an **agent** and what distinguishes it from a chatbot.
- Explain **tool use** and how **skills** expose safe actions to an agent.
- Have an agent operate the lakehouse via the `./lakehouse` CLI.
- Describe the guardrails that make agent-operated infrastructure safe.

## 12.2 What an agent is

> **Agent** *(canonical term)*: an LLM that takes *actions* through tools to
> accomplish a goal — not just generating text, but doing work and observing results.

A chatbot answers; an agent *acts*. Given a goal ("run today's pipeline and tell me
revenue by status"), an agent plans steps, calls tools to execute them, reads the
results, and adapts — looping until the goal is met. The lakehouse is an ideal thing
for an agent to operate because it already exposes a clean, uniform control
surface: the `./lakehouse` CLI you've used in every chapter.

> **Tool use / skills** *(canonical term)*: the defined actions an agent can invoke.
> Here the tools are `./lakehouse` subcommands, and **skills** are documented
> procedures (`.claude/skills/`) that tell the agent how to accomplish a task.

![How an agent operates the lakehouse](../figures/ch12/fig-12.1-agent-loop.svg)

**Figure 12.1** — The agent loop: goal → plan → call a tool (`./lakehouse …`) →
observe output → repeat until done. Skills supply the how-to; the CLI supplies the
actions.

## 12.3 Why the lakehouse is agent-ready

Two things make this lakehouse operable by an agent without inventing anything:

1. **One uniform control surface.** Everything — setup, start/stop, status,
   testdata, pipelines, browse — goes through `./lakehouse`. An agent that can run
   shell commands can run the whole platform, and `status --json` gives it
   machine-readable state to reason over.
2. **Skills as documented procedures.** The repo ships `.claude/skills/` (e.g. the
   `SDP.md` skill) and a `CLAUDE.md` entry point. A skill is a written recipe — "to
   run the medallion, do X, then Y, verify Z" — so the agent follows a proven path
   instead of improvising.

### Under the hood — the README contract

The reference architecture's demos follow a fixed README contract: each demo
documents how to run and verify it in a predictable place. That consistency is what
lets an agent "run any demo by reading its README" — the structure *is* the
tool interface. Well-structured docs aren't just for humans anymore; they're the
API an agent programs against.

## 12.4 Build — have an agent run the lakehouse

Step 1 — point a coding agent (Claude Code, Cursor, or similar) at the repo.
The agent discovers its instructions from `CLAUDE.md` / `AGENTS.md` and the
`.claude/skills/` directory — the same files you've had all along.

Step 2 — give it a plain-English goal:

```
You: Start the lakehouse stack, run the batch medallion pipeline,
     then show me daily revenue by status from the Gold table.
```

Step 3 — watch the agent operate. It plans and calls tools in sequence,
observing each result before the next step:

```text
agent → ./lakehouse start spark          # bring up compute
agent → ./lakehouse start unity-catalog  # bring up the catalog
agent → ./lakehouse status --json        # confirm healthy (machine-readable)
agent → spark-pipelines run --spec .../spark-pipeline.yml   # run the medallion
agent → ./lakehouse browsedata iceberg.gold.daily_revenue   # read the result
agent → "Here's daily revenue by status: …"                 # reports back
```

Expected: the agent brings the stack up, runs the pipeline you built in Chapter 7,
reads the Gold table through the catalog, and summarizes the numbers — all from one
English instruction, using only the tools you already have.

Step 4 — try a follow-up that requires adaptation, e.g. "now tear it all
down cleanly." The agent uses `./lakehouse stop all`, confirming state via `status`
before and after — planning and verifying, not just firing commands.

## 12.5 Guardrails: agent-operated, not agent-out-of-control

Handing infrastructure to an agent demands limits. The practitioner guardrails:

- **Scoped tools.** The agent gets the `./lakehouse` CLI, not unrestricted shell
  power. Its action space is the platform's own commands.
- **Read-then-act.** `status --json` lets the agent *check* state before and after
  changes, so it verifies rather than assumes.
- **Reversibility.** Teardown is one command and volumes are preserved by default —
  a mistake is recoverable, echoing the time-travel safety net from Chapter 5.
- **Human in the loop for destructive steps.** Operations like `reset --all` (which
  wipes data) should require explicit confirmation, not silent execution.

## 12.6 Checkpoint

- You pointed an agent at the repo; it read `CLAUDE.md` / skills for instructions.
- From one English goal, the agent started the stack, ran the medallion, and
  reported Gold results using `./lakehouse` tools.
- It used `status` to verify state before/after actions.
- You can name three guardrails that keep agent operation safe.

## 12.7 Recap & what's next

- An **agent** takes actions through **tools** to reach a goal; **skills** give it
  proven procedures.
- The lakehouse is agent-ready because of one uniform CLI control surface and
  documented skills.
- Guardrails — scoped tools, read-then-act, reversibility, human approval for
  destructive ops — keep it safe.
- **Next — Chapter 13, Deploy & Teardown:** take the lakehouse to the cloud (tour),
  tear it down cleanly, and map where to go next.

![Progress: Agents complete, Deploy next](../figures/ch12/fig-12.4-progress-agents-done.svg)

**Figure 12.4** — Progress map with **Agents ✓** and **Deploy** next.
