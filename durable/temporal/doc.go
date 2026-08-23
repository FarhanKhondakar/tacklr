// Package temporal is the Temporal.io durable-execution backend for the
// harness. It implements durable.Executor so async spawn_worker jobs (and,
// later, session turns) survive process restarts with Temporal-managed
// retries, signals, and visibility. Temporal is opt-in: nothing in the core
// harness, server, or durable abstraction imports this package.
//
// Wiring (host-owned):
//
//	cfg := temporal.ConfigFromEnv()            // TEMPORAL_ADDRESS, ...
//	runner, err := tacklr.DurableStepRunner(factory) // rebuilds a worker per step
//	host, err := temporal.NewWorkerHost(ctx, cfg, runner)
//	go host.Start()                            // polls the task queue
//	opts := tacklr.AgentOptions{
//	    ...,
//	    Durable: host.Executor(),             // same client as the worker
//	}
//
// A DurableStepRunnerFactory builds the AgentOptions for one worker step from
// the durable spec plus host configuration (model, tools, store, inherited
// session world). It runs inside the worker process, so it must not close over
// a parent harness.
//
// Deployment note: run the worker in the same process as the server (via
// WorkerHost) to keep live event bridging; a separately deployed worker
// degrades event delivery to history replay.
package temporal
