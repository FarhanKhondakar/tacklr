// Package temporal is the Temporal.io durable-execution backend for the
// harness. It implements durable.Executor so async spawn_worker jobs and
// session turns survive process restarts with Temporal-managed retries,
// signals, and visibility. Temporal is opt-in: nothing in the core harness,
// server, or durable abstraction imports this package.
//
// Wiring (host-owned):
//
//	cfg := temporal.ConfigFromEnv()            // TEMPORAL_ADDRESS, ...
//	workerRunner, _ := tacklr.DurableStepRunner(workerFactory) // rebuilds a worker per step
//	turnRunner, _ := reg.SessionTurnStepRunner()              // registry turns durably
//	runner := func(ctx, in) (durable.StepOutcome, error) {
//	    if in.Spec.Kind == durable.RunKindSessionTurn { return turnRunner(ctx, in) }
//	    return workerRunner(ctx, in)
//	}
//	host, err := temporal.NewWorkerHost(ctx, cfg, runner)
//	go host.Start()                            // polls the task queue
//	reg = server.NewRegistry(store, agent, server.WithDurable(host.Executor()))
//
// A DurableStepRunnerFactory builds the AgentOptions for one worker step from
// the durable spec plus host configuration (model, tools, store, inherited
// session world). It runs inside the worker process, so it must not close over
// a parent harness.
//
// Turn semantics: a session turn runs as a turn-scoped workflow. When the
// harness parks on an interrupt, the workflow ends with StatusInterrupted and
// session/resume starts a fresh turn-scoped workflow carrying the resolutions.
// The registry reconstructs from the Postgres checkpoint each run, so activity
// retries after a crash are idempotent.
//
// Deployment note: run the worker in the same process as the server (via
// WorkerHost) to keep live event bridging; a separately deployed worker
// degrades event delivery to history replay.
package temporal
