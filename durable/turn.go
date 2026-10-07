package durable

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
)

// Wake kinds from Exec.Recv.
const (
	WakePrompt = "prompt"
	WakeResume = "resume"
	WakeCancel = "cancel"
	WakeClose  = "close"
)

// PromptIn is one prompt delivered to a waiting session.
type PromptIn struct {
	Text        string
	UserMessage *tacklr.Message
	AgentID     string
	MCPServers  []mcp.MCPConfig
	Auth        AuthContext
	State       map[string]any
}

// ResumeIn is one resume delivered to a parked session.
type ResumeIn struct {
	Responses map[string][]byte
	Auth      AuthContext
	State     map[string]any
}

// Wake is the next mailbox event.
type Wake struct {
	Kind   string
	Prompt PromptIn
	Resume ResumeIn
}

// InferenceInput is one model step. Rec is the snapshot row to persist.
type InferenceInput struct {
	SessionID     SessionID
	Rec           Snapshot
	MCPServers    []mcp.MCPConfig
	State         map[string]any
	User          *tacklr.Message
	Extra         []*tacklr.Message
	HadToolRound  bool
	ModelRequests int
	Resume        map[string][]byte
}

// InferenceOutput is the model step result.
type InferenceOutput struct {
	Complete  bool
	ToolCalls []tacklr.ToolCall
	Result    string
}

// ToolInput is one tool step.
type ToolInput struct {
	SessionID  SessionID
	Rec        Snapshot
	MCPServers []mcp.MCPConfig
	State      map[string]any
	Call       tacklr.ToolCall
}

// ToolOutput is one tool step. Job fields are the child the runtime must
// start or cancel. AwaitID blocks this tool call until that child finishes.
type ToolOutput struct {
	Interrupted   bool
	InterruptID   string
	InterruptData []byte
	CancelID      SessionID
	AwaitID       SessionID
	JobID         SessionID
	JobName       string
	JobTask       string
	Child         bool
}

// CommitInput writes a tool output the runtime already has (a child result)
// without running the tool again.
type CommitInput struct {
	SessionID  SessionID
	Rec        Snapshot
	MCPServers []mcp.MCPConfig
	State      map[string]any
	Call       tacklr.ToolCall
	Output     string
}

// Exec is the part of a session that depends on the durable system.
// Run uses it for steps, the mailbox, and child sessions.
// Inference, Tool, and Commit retry a network error inside the implementation.
// Any other error is final.
type Exec interface {
	Infer(InferenceInput) (InferenceOutput, error)
	Tool(ToolInput) (ToolOutput, error)
	Commit(CommitInput) error
	Emit(tacklr.StreamEvent)

	// DrainPrompts takes prompts that arrived during a step.
	DrainPrompts() []PromptIn

	ChildIDs() []SessionID
	CancelAll()
	ApplyChild(ToolOutput) error
	AwaitChild(SessionID) (string, error)
	Harvest(inbox *[]*tacklr.Message)
	WaitJobs(t *Turn, state *map[string]any) error
}

// Turn is one session's scheduler state. The snapshot holds the conversation.
// Run is one prompt or resume, and returns when the turn parks, finishes, or fails.
type Turn struct {
	SessionID  SessionID
	AgentID    string
	Specialist string
	Worker     string
	Parent     SessionID

	Mounts    []MountRecipe
	MCP       []mcp.MCPConfig
	Inbox     []*tacklr.Message
	NextAgent string
	NextMCP   []mcp.MCPConfig
	// Seed is CreateSession state until the first Run consumes it.
	Seed map[string]any

	HadTools bool
	Requests int
	Leftover []tacklr.ToolCall
	Parked   bool
	// Overlay is the turn's user state while it is parked.
	Overlay map[string]any

	Result   string
	Terminal SessionState
	Err      error
	Yielded  bool
	Closed   bool
}

// Run drives one turn. user is the prompt. resume is set when continuing a park.
func (t *Turn) Run(x Exec, user *tacklr.Message, resume map[string][]byte, auth AuthContext, extra map[string]any) {
	if len(resume) == 0 {
		t.HadTools = false
		t.Requests = 0
		t.Leftover = nil
		t.Parked = false
	}
	turnState := MergeUserState(MergeUserState(t.Seed, t.Overlay), extra)
	t.Seed = nil
	t.Overlay = nil
	t.Mounts = ApplyAuth(t.Mounts, auth)
	t.Terminal = ""
	t.Err = nil
	t.Yielded = false
	t.Result = ""

	rec := func() Snapshot {
		return Snapshot{
			AgentID:    t.AgentID,
			Specialist: t.Specialist,
			Worker:     t.Worker,
			Parent:     t.Parent,
			Children:   x.ChildIDs(),
			Mounts:     t.Mounts,
		}
	}
	fail := func(err error) {
		if err == nil {
			return
		}
		t.Err = err
		t.Terminal = SessionFailed
		msg := err.Error()
		if errors.Is(err, context.Canceled) {
			msg = context.Canceled.Error()
			t.Inbox = nil
			t.NextAgent = ""
			t.NextMCP = nil
			x.CancelAll()
		}
		x.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
	}

	var extraUsers []*tacklr.Message
	toolCalls := []tacklr.ToolCall(nil)
	inferComplete := false
	interruptID := ""
	interruptData := []byte(nil)

	if len(resume) > 0 {
		out, err := x.Infer(InferenceInput{
			SessionID:  t.SessionID,
			Rec:        rec(),
			MCPServers: t.MCP,
			Resume:     resume,
			State:      turnState,
		})
		if err != nil {
			fail(err)
			return
		}
		toolCalls = slices.Concat(out.ToolCalls, t.Leftover)
		t.Leftover = nil
		t.Parked = false
	}

	for {
		if len(toolCalls)+len(t.Leftover) == 0 && !t.Parked {
			x.Harvest(&t.Inbox)
			if user == nil {
				for _, p := range x.DrainPrompts() {
					t.Steer(p, &turnState)
				}
			}
			if len(t.Inbox) > 0 {
				extraUsers = append(extraUsers, t.Inbox...)
				t.Inbox = nil
				inferComplete = false
			}
		}
		switch tacklr.Next(len(toolCalls), t.Parked, inferComplete, len(x.ChildIDs()) > 0) {
		case tacklr.ActionInfer:
			out, err := x.Infer(InferenceInput{
				SessionID:     t.SessionID,
				Rec:           rec(),
				MCPServers:    t.MCP,
				User:          user,
				Extra:         extraUsers,
				HadToolRound:  t.HadTools,
				ModelRequests: t.Requests,
				State:         turnState,
			})
			user = nil
			extraUsers = nil
			if err != nil {
				fail(err)
				return
			}
			t.Requests++
			if out.Complete {
				inferComplete = true
				t.Result = out.Result
				toolCalls = nil
				continue
			}
			inferComplete = false
			toolCalls = out.ToolCalls
		case tacklr.ActionRunTools:
			t.HadTools = true
			tc := toolCalls[0]
			rest := toolCalls[1:]
			tout, err := x.Tool(ToolInput{
				SessionID:  t.SessionID,
				Rec:        rec(),
				MCPServers: t.MCP,
				Call:       tc,
				State:      turnState,
			})
			if err != nil {
				fail(err)
				return
			}
			if err := x.ApplyChild(tout); err != nil {
				fail(err)
				return
			}
			if tout.AwaitID != "" {
				output, err := x.AwaitChild(tout.AwaitID)
				if err != nil {
					fail(err)
					return
				}
				if err := x.Commit(CommitInput{
					SessionID:  t.SessionID,
					Rec:        rec(),
					MCPServers: t.MCP,
					Call:       tc,
					Output:     output,
					State:      turnState,
				}); err != nil {
					fail(err)
					return
				}
				tout.Interrupted = false
			}
			if tout.Interrupted {
				t.Leftover = rest
				interruptID = tout.InterruptID
				interruptData = tout.InterruptData
				t.Parked = true
				toolCalls = nil
				continue
			}
			toolCalls = rest
		case tacklr.ActionYield:
			t.Yielded = true
			t.Overlay = turnState
			x.Emit(tacklr.StreamEvent{
				Type:      tacklr.StreamEventInterrupt,
				MessageID: interruptID,
				Data:      interruptData,
			})
			return
		case tacklr.ActionComplete:
			t.Terminal = SessionComplete
			x.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventComplete})
			return
		case tacklr.ActionWait:
			err := x.WaitJobs(t, &turnState)
			if err == nil {
				break
			}
			fail(err)
			return
		}
	}
}

// Steer queues a prompt onto this turn and merges its auth and state.
func (t *Turn) Steer(p PromptIn, state *map[string]any) {
	msg := p.UserMessage
	if msg == nil {
		msg = &tacklr.Message{Role: tacklr.RoleUser, Content: p.Text}
	}
	t.Inbox = append(t.Inbox, msg)
	t.Mounts = ApplyAuth(t.Mounts, p.Auth)
	if state != nil {
		*state = MergeUserState(*state, p.State)
	}
	if p.AgentID != "" {
		t.NextAgent = p.AgentID
	}
	if p.MCPServers != nil {
		t.NextMCP = p.MCPServers
	}
}

// MergeUserState copies overlay onto a clone of base. Overlay wins on conflict.
func MergeUserState(base, overlay map[string]any) map[string]any {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := make(map[string]any, len(base)+len(overlay))
	maps.Copy(out, base)
	maps.Copy(out, overlay)
	return out
}
