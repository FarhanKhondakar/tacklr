package temporal

import (
	"time"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/mcp"
)

const (
	signalPrompt = "Prompt"
	signalResume = "Resume"
	signalCancel = "Cancel"
	signalClose  = "Close"

	queryStatus   = "tacklr_status"
	queryChildren = "tacklr_children"

	signalChildWaiting = "ChildWaiting"
)

// workflowInput is wait-loop start state (scheduler), not a Snapshot.
// Credentials live in SecretStorage. userState seed merges into the
// checkpoint on the first activity save.
type workflowInput struct {
	SessionID  durable.SessionID
	AgentID    string
	MCPServers []mcp.MCPConfig
	Mounts     []durable.MountRecipe
	// TurnLocalityTimeout, when > 0, pins the turn's activities to one worker
	// (Temporal CreateSession). Zero skips worker sessions: activities can run
	// on any worker. There is no default timeout.
	TurnLocalityTimeout time.Duration
	// ActivityTimeout is StartToCloseTimeout for Inference/Tool activities.
	// Zero means 10 minutes (resolveActivityTimeout).
	ActivityTimeout time.Duration
	// HeartbeatTimeout is the activity heartbeat timeout. Zero means 30 seconds.
	HeartbeatTimeout time.Duration
	// ActivityAttempts is Temporal MaximumAttempts for a wrapped network
	// error, a model refusal, or a stale checkpoint. Zero means 3.
	ActivityAttempts int32
	// Prompt, when set, runs one turn then completes the workflow (spawn_specialist child).
	Prompt     string
	Parent     durable.SessionID
	Specialist string
	Worker     string
	// State is CreateSession.State, already JSON-roundtripped. Overlay onto
	// the checkpoint; not a durable workflow copy of userState.
	State map[string]any
}

type waitSignal struct {
	kind   string
	prompt durable.PromptIn
	resume durable.ResumeIn
}
