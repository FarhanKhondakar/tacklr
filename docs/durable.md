# Durable execution with Temporal.io

Tacklr can run background `spawn_worker` jobs and session turns through a
durable executor so they survive process restarts. Temporal.io is the first
backend; the `durable` abstraction is engine-agnostic (Azure / AWS later).

Durable execution is **opt-in**: nothing changes until you wire an executor.

## Concepts

- **Executor** — starts durable runs (`durable.Executor`). A nil executor on
  the harness/server means today's in-process behavior.
- **Run** — a worker job or a session turn. Each run is a single Temporal
  workflow execution with a stable run id (job id, or `threadID#<uuid>`).
- **Step** — one full harness `Run`/`ReturnFromInterrupt`, executed as a
  Temporal activity (`tacklr.RunStep`). The harness is rebuilt from its
  Postgres checkpoint per step, so a retry after a crash is idempotent.
- **Signal** — resolves a parked interrupt for worker jobs (`tacklr.resume`).
  Session turns instead end with `StatusInterrupted`; `session/resume` starts a
  fresh turn-scoped workflow carrying the resolutions.
- **Hybrid live + replay** — when the worker runs in-process with the server
  (recommended), step events stream live through an in-process bridge while
  also being recorded in workflow history. `Seq` cursors make the replay→live
  join lossless. A separately deployed worker degrades to history replay.

## Run it

### 1. Start Temporal (dev server)

```bash
temporal server start-dev --ui --port 7233 --ui-port 8233
# workflow visibility UI at http://localhost:8233
```

### 2. Start Postgres (checkpoints must survive restarts)

```bash
docker run -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
```

Create the `session` table from `stores/testdata/session_schema.sql`.

### 3. Run the harness

```bash
OPENAI_API_KEY=... OPENAI_BASE_URL=... OPENAI_MODEL=... \
TACKLR_POSTGRES_URL=postgres://postgres:postgres@localhost:5432/postgres \
TEMPORAL_ADDRESS=127.0.0.1:7233 \
go run ./cmd/testserver --http
```

Setting any `TEMPORAL_*` variable enables durable execution and registers a
`researcher` subagent.

## Drive it

`session/prompt` starts a turn-scoped workflow; `session/cancel` cancels it.
From an ACP client (e.g. the `acp` Python SDK over `http://localhost:3000/acp`):

```text
session/new           → session id
session/prompt        → "Use spawn_worker with block=false and
                         worker_name=researcher to research X, then get_job."
```

While the job runs, kill the Go server (`Ctrl-C`), restart step 3, and re-prompt
with `get_job bg1 block=true` — the result arrives from the surviving Temporal
run, rebuilt from the Postgres checkpoint. The whole lifecycle is visible in the
Temporal UI.

## Wiring in your host

```go
workerRunner, _ := tacklr.DurableStepRunner(workerFactory) // rebuilds a worker per step
turnRunner, _ := reg.SessionTurnStepRunner()              // registry turns durably
runner := func(ctx context.Context, in durable.StepInput) (durable.StepOutcome, error) {
    if in.Spec.Kind == durable.RunKindSessionTurn {
        return turnRunner(ctx, in)
    }
    return workerRunner(ctx, in)
}
host, err := temporal.NewWorkerHost(ctx, temporal.ConfigFromEnv(), runner)
if err != nil { ... }
if err := host.StartBackground(); err != nil { ... }
defer host.Close()
reg = server.NewRegistry(store, agent, server.WithDurable(host.Executor()))
```

Config is environment-driven (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`,
`TEMPORAL_TASK_QUEUE`, `TEMPORAL_API_KEY`, `TEMPORAL_TLS_CERT`, `TEMPORAL_TLS_KEY`).

## Behavioral guarantees

- **Worker process dies** → the workflow keeps running server-side; the
  activity heartbeats time out, then retries onto the next worker (checkpoint
  makes the retry safe).
- **Parent harness `Close()`** → durable jobs are skipped by
  `cancelBackgroundJobs`; the backend owns their lifecycle.
- **Process restart** → a reloaded harness re-attaches to surviving runs from
  the checkpointed open-job list, so `get_job`/`list_jobs` keep working.
- **Session turn interrupt** → the workflow ends with `StatusInterrupted`;
  `session/resume` starts a new turn-scoped workflow carrying the resolutions.
