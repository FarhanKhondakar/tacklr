package livesess

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/temporal"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

var traceOnce sync.Once

func trace(t testing.TB) {
	t.Helper()
	traceOnce.Do(func() {
		if _, err := telemetry.Init(context.Background(), telemetry.Config{}); err != nil {
			t.Fatal(err)
		}
	})
}

// Runtime starts a worker against the shared Temporal Docker server.
func Runtime(t testing.TB, agent tacklr.AgentOptions) durable.Runtime {
	t.Helper()
	trace(t)
	c, err := temporal.Dial(client.Options{HostPort: temporaldocker.HostPort(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cfg := temporal.Config{
		Agent:          agent,
		TaskQueue:      "tacklr-" + uuid.NewString(),
		Snapshots:      durable.NewMemorySnapshot(),
		Fallback:       durable.NewMemoryEventLog(),
		Secrets:        durable.NewMemorySecretStorage(),
		Projection:     vfs.DirectProjection{},
		DisableStreams: true,
	}
	rt := temporal.New(c, cfg)
	w := temporal.NewWorker(c, cfg)
	stop := make(chan any)
	done := make(chan struct{})
	go func() {
		_ = w.Run(stop)
		close(done)
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})
	return rt
}
