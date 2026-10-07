// Package temporal is the Temporal adapter for durable.Runtime.
// Hosts use Dial, New, and NewWorker with the same Config (including
// Snapshots and Secrets). NewWorker registers SessionWorkflow and the
// turn activities.
package temporal

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr/telemetry"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/vfs"
)

// Runtime implements durable.Runtime with one Temporal workflow per session.
type Runtime struct {
	client              client.Client
	taskQueue           string
	agent               tacklr.AgentOptions
	fallback            durable.EventLog
	snapshots           durable.SnapshotStore
	disableStreams      bool
	turnLocalityTimeout time.Duration
	activityTimeout     time.Duration
	heartbeatTimeout    time.Duration
	activityAttempts    int32
	secrets             durable.SecretStorage
	jobs                map[string]durable.JobHandler

	mu     sync.Mutex
	closed map[durable.SessionID]struct{}
}

const (
	defaultActivityTimeout  = 10 * time.Minute
	defaultHeartbeatTimeout = 30 * time.Second
	defaultActivityAttempts = 3
)

// Config is the single Temporal host config for New and NewWorker.
type Config struct {
	// Agent is the one agent this worker runs. Specialists on Agent.Options
	// are assistants it can spawn. Jobs are named handlers, not agents.
	Agent     tacklr.AgentOptions
	TaskQueue string
	// Snapshots is the session record. Required. New and NewWorker must share
	// the same instance. Tokens never go here.
	Snapshots  durable.SnapshotStore
	Fallback   durable.EventLog
	Projection vfs.Projection
	// DisableStreams uses the fallback EventLog instead of Workflow Streams.
	DisableStreams bool
	// TurnLocality, when > 0, pins a turn's activities to one worker.
	TurnLocality time.Duration
	// ActivityTimeout is Inference/Tool StartToCloseTimeout. Zero is 10 minutes.
	ActivityTimeout time.Duration
	// HeartbeatTimeout is the activity heartbeat timeout. Zero is 30 seconds.
	HeartbeatTimeout time.Duration
	// ActivityAttempts is Temporal MaximumAttempts for a wrapped network
	// error, a model refusal, or a stale checkpoint. Zero is 3. 1 means no
	// retry. Any other activity error stops on the first attempt.
	ActivityAttempts int32
	// Secrets holds work-item credentials for activities. Required. New and
	// NewWorker must share the same instance. Tokens never enter event history.
	Secrets durable.SecretStorage
	// Jobs are named background workers Schedule can start. Specialist
	// names on the catalog take precedence.
	Jobs map[string]durable.JobHandler
}

func (c Config) queue() string {
	if c.TaskQueue == "" {
		return "tacklr"
	}
	return c.TaskQueue
}

func requireCfg(cfg Config) {
	opts := cfg.Agent
	if opts.SessionID != "" || opts.MountSession != nil || opts.SkillsSession != nil {
		panic("temporal: Agent cannot set SessionID, MountSession, or SkillsSession; the runtime injects those per turn")
	}
	if cfg.Snapshots == nil {
		panic("temporal: Snapshots is required")
	}
	if cfg.Secrets == nil {
		panic("temporal: Secrets is required")
	}
}

func (c Config) eventLog() durable.EventLog {
	if c.Fallback != nil {
		return c.Fallback
	}
	return durable.NewMemoryEventLog()
}

// New constructs a Temporal Runtime. The host must also run NewWorker on the
// same Config, including the same Snapshots and Secrets stores.
func New(c client.Client, cfg Config) *Runtime {
	if c == nil {
		panic("temporal: Client is required")
	}
	requireCfg(cfg)
	return &Runtime{
		client:              c,
		taskQueue:           cfg.queue(),
		agent:               cfg.Agent,
		fallback:            cfg.eventLog(),
		snapshots:           cfg.Snapshots,
		disableStreams:      cfg.DisableStreams,
		turnLocalityTimeout: cfg.TurnLocality,
		activityTimeout:     cmp.Or(cfg.ActivityTimeout, defaultActivityTimeout),
		heartbeatTimeout:    cmp.Or(cfg.HeartbeatTimeout, defaultHeartbeatTimeout),
		activityAttempts:    cmp.Or(cfg.ActivityAttempts, defaultActivityAttempts),
		secrets:             cfg.Secrets,
		jobs:                cfg.Jobs,
		closed:              make(map[durable.SessionID]struct{}),
	}
}

// CreateSession implements durable.Runtime.
func (r *Runtime) CreateSession(ctx context.Context, req durable.CreateSession) (durable.SessionID, error) {
	if req.Worker != "" && req.Specialist != "" {
		return "", fmt.Errorf("specialist and worker are exclusive: %w", tacklr.ErrInvalid)
	}
	if req.Worker != "" {
		if _, ok := r.jobs[req.Worker]; !ok {
			return "", fmt.Errorf("%w: %s", tacklr.ErrNotFound, req.Worker)
		}
	}
	id := req.SessionID
	if id == "" {
		id = durable.SessionID(uuid.NewString())
	}
	seed, err := adapter.EncodeUserState(req.State)
	if err != nil {
		return "", err
	}
	_, err = r.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        string(id),
		TaskQueue: r.taskQueue,
	}, SessionWorkflow, workflowInput{
		SessionID:           id,
		MCPServers:          mcp.DurableConfigs(req.MCPServers),
		Mounts:              req.Mounts,
		TurnLocalityTimeout: r.turnLocalityTimeout,
		ActivityTimeout:     r.activityTimeout,
		HeartbeatTimeout:    r.heartbeatTimeout,
		ActivityAttempts:    r.activityAttempts,
		State:               seed,
		Parent:              req.Parent,
		Specialist:          req.Specialist,
		Worker:              req.Worker,
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

func (r *Runtime) markClosed(id durable.SessionID) {
	r.mu.Lock()
	r.closed[id] = struct{}{}
	r.mu.Unlock()
}

func (r *Runtime) isClosed(id durable.SessionID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.closed[id]
	return ok
}

func (r *Runtime) signal(ctx context.Context, id durable.SessionID, name string, arg any) error {
	if name != signalClose && r.isClosed(id) {
		return durable.ErrSessionNotFound
	}
	if err := r.client.SignalWorkflow(ctx, string(id), "", name, arg); err != nil {
		return durable.ErrSessionNotFound
	}
	return nil
}

func (r *Runtime) Prompt(ctx context.Context, sessionID durable.SessionID, msg durable.Prompt) error {
	encoded, err := adapter.EncodeUserState(msg.State)
	if err != nil {
		return err
	}
	if err := r.secrets.Put(ctx, sessionID, durable.Secrets{Auth: msg.Auth}); err != nil {
		return err
	}
	return r.signal(ctx, sessionID, signalPrompt, durable.PromptIn{
		Text:        msg.Text,
		UserMessage: msg.UserMessage,
		MCPServers:  mcp.DurableConfigs(msg.MCPServers),
		Auth:        msg.Auth.WithoutSecrets(),
		State:       encoded,
	})
}

func (r *Runtime) Resume(ctx context.Context, sessionID durable.SessionID, resume durable.Resume) error {
	encoded, err := adapter.EncodeUserState(resume.State)
	if err != nil {
		return err
	}
	if err := r.secrets.Put(ctx, sessionID, durable.Secrets{Auth: resume.Auth}); err != nil {
		return err
	}
	return r.signal(ctx, sessionID, signalResume, durable.ResumeIn{
		Responses: resume.Responses,
		Auth:      resume.Auth.WithoutSecrets(),
		State:     encoded,
	})
}

// Cancel implements durable.Runtime.
func (r *Runtime) Cancel(ctx context.Context, sessionID durable.SessionID) error {
	cancelLiveTurn(sessionID)
	return r.signal(ctx, sessionID, signalCancel, nil)
}

// Close implements durable.Runtime.
func (r *Runtime) Close(ctx context.Context, sessionID durable.SessionID) error {
	kids, _ := r.Children(ctx, sessionID)
	r.markClosed(sessionID)
	_ = r.signal(ctx, sessionID, signalClose, nil)
	for _, k := range kids {
		durable.DeleteSessionMessages(ctx, r.agent, k)
		_ = r.secrets.Delete(ctx, k)
	}
	durable.DeleteSessionMessages(ctx, r.agent, sessionID)
	_ = r.secrets.Delete(ctx, sessionID)
	_ = r.snapshots.Delete(ctx, sessionID)
	_ = r.fallback.CloseSession(ctx, sessionID)
	return nil
}

type sub struct {
	ch     <-chan tacklr.StreamEvent
	cancel context.CancelFunc
}

func (s *sub) Events() <-chan tacklr.StreamEvent { return s.ch }
func (s *sub) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Head implements durable.Runtime. When Workflow Streams is on, this is the
// stream's next offset so Subscribe(after Head) skips prior-turn events.
func (r *Runtime) Head(ctx context.Context, sessionID durable.SessionID) (durable.Seq, error) {
	if !r.disableStreams {
		val, err := r.client.QueryWorkflow(ctx, string(sessionID), "", workflowstreams.OffsetQueryName)
		if err == nil {
			var n int64
			if err := val.Get(&n); err == nil && n >= 0 {
				return durable.Seq(n), nil //nolint:gosec // G115: stream offsets are well below MaxUint64
			}
		}
	}
	return r.fallback.Head(ctx, sessionID)
}

// Subscribe implements durable.Runtime.
func (r *Runtime) Subscribe(ctx context.Context, sessionID durable.SessionID, after durable.Seq) (durable.Subscription, error) {
	subCtx, cancel := context.WithCancel(ctx)
	if r.disableStreams {
		src, err := r.fallback.Subscribe(subCtx, sessionID, after)
		if err != nil {
			cancel()
			return nil, err
		}
		ch := make(chan tacklr.StreamEvent)
		go func() {
			defer close(ch)
			for ev := range src {
				if !deliver(subCtx, ch, ev) {
					return
				}
			}
		}()
		return &sub{ch: ch, cancel: cancel}, nil
	}
	c := workflowstreams.NewClient(r.client, string(sessionID), workflowstreams.Options{})
	ch := make(chan tacklr.StreamEvent, 64)
	dc := converter.GetDefaultDataConverter()
	go func() {
		defer close(ch)
		defer func() { _ = c.Close(subCtx) }()
		off := int64(after) //nolint:gosec // G115: EventLog seq is well below MaxInt64
		for item, err := range c.Subscribe(subCtx, workflowstreams.SubscribeOptions{
			Topics:     []string{durable.TopicEvents},
			FromOffset: off,
		}) {
			if err != nil {
				return
			}
			var ev tacklr.StreamEvent
			if err := dc.FromPayload(item.Data, &ev); err != nil {
				return
			}
			if !deliver(subCtx, ch, ev) {
				return
			}
		}
	}()
	return &sub{ch: ch, cancel: cancel}, nil
}

func deliver(ctx context.Context, ch chan<- tacklr.StreamEvent, ev tacklr.StreamEvent) bool {
	if ev.Error == nil && ev.Fail != "" {
		ev.Error = failFromWire(ev.Fail)
	}
	select {
	case ch <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func failFromWire(s string) error {
	for _, sent := range []error{
		tacklr.ErrModelRefused,
		tacklr.ErrMaxTokens,
		tacklr.ErrMaxTurnRequests,
		context.Canceled,
	} {
		if strings.Contains(s, sent.Error()) {
			return sent
		}
	}
	return errors.New(s)
}

// Children implements durable.Runtime.
func (r *Runtime) Children(ctx context.Context, parent durable.SessionID) ([]durable.SessionID, error) {
	if r.isClosed(parent) {
		return nil, durable.ErrSessionNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(parent), "", queryChildren)
	if err != nil {
		return nil, durable.ErrSessionNotFound
	}
	var ids []durable.SessionID
	_ = val.Get(&ids)
	return ids, nil
}

// Jobs implements durable.Runtime.
func (r *Runtime) Jobs(ctx context.Context, parent durable.SessionID) ([]durable.SessionStatus, error) {
	ids, err := r.Children(ctx, parent)
	if err != nil {
		return nil, err
	}
	out := make([]durable.SessionStatus, 0, len(ids))
	for _, id := range ids {
		st, err := r.Status(ctx, id)
		if err != nil {
			continue
		}
		out = append(out, st)
	}
	return out, nil
}

// Status implements durable.Runtime.
func (r *Runtime) Status(ctx context.Context, id durable.SessionID) (durable.SessionStatus, error) {
	st := durable.SessionStatus{ID: id, State: durable.SessionUnknown}
	if r.isClosed(id) {
		return st, durable.ErrSessionNotFound
	}
	val, err := r.client.QueryWorkflow(ctx, string(id), "", queryStatus)
	if err != nil {
		return st, durable.ErrSessionNotFound
	}
	_ = val.Get(&st)
	return st, nil
}

// Dial is client.Dial with Temporal's OpenTelemetry v2 plugin prepended.
// It installs a replay-safe tracer on the process. When telemetry.Init has
// already run, that tracer keeps the same export configuration.
func Dial(opts client.Options) (client.Client, error) {
	if _, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider); !ok {
		if !telemetry.TracerInstalled() {
			otel.SetTracerProvider(temporalotel.NewReplaySafeTracerProvider())
		} else if err := telemetry.ReinstallTracer(context.Background(), func(o ...sdktrace.TracerProviderOption) (trace.TracerProvider, func(context.Context) error) {
			tp := temporalotel.NewReplaySafeTracerProvider(o...)
			return tp, tp.Shutdown
		}); err != nil {
			return nil, err
		}
	}
	plugin, err := temporalotel.NewPlugin(temporalotel.PluginOptions{})
	if err != nil {
		return nil, err
	}
	opts.Plugins = append([]client.Plugin{plugin}, opts.Plugins...)
	return client.Dial(opts)
}

// NewWorker returns a Temporal worker with EnableSessionWorker and
// SessionWorkflow plus Inference, Tool, CommitToolOutput, and EmitEvent
// activities. Pass the same Config as New, including Snapshots and Secrets.
func NewWorker(c client.Client, cfg Config) worker.Worker {
	requireCfg(cfg)
	w := worker.New(c, cfg.queue(), worker.Options{
		EnableSessionWorker:               true,
		MaxConcurrentSessionExecutionSize: 1000,
	})
	proj := cfg.Projection
	if proj == nil {
		proj = vfs.FuseProjection{}
	}
	fallback := cfg.eventLog()
	acts := &activities{
		Agent:          cfg.Agent,
		Snapshots:      cfg.Snapshots,
		Projection:     proj,
		Fallback:       fallback,
		DisableStreams: cfg.DisableStreams,
		Secrets:        cfg.Secrets,
		Jobs:           cfg.Jobs,
	}
	w.RegisterWorkflow(SessionWorkflow)
	w.RegisterActivity(acts)
	return w
}
