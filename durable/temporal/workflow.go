package temporal

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// SessionWorkflow is the Temporal type name for the session wait loop.
// NewWorker registers it. Hosts call durable.Runtime, not this function.
func SessionWorkflow(ctx workflow.Context, in workflowInput) (string, error) {
	logger := workflow.GetLogger(ctx)
	if _, err := workflowstreams.NewWorkflowStream(ctx, nil); err != nil {
		logger.Error("workflow stream", "error", err)
	}

	var (
		closed      bool
		agentID     = in.AgentID
		mcpServers  = in.MCPServers
		mounts      = durable.ApplyAuth(in.Mounts, durable.AuthContext{})
		spawned     []childRun
		inbox       []*tacklr.Message
		nextAgentID string
		nextMCP     []mcp.MCPConfig
		yielded     bool
		result      string
		terminal    durable.SessionState
		seed        = in.State
		promptCh    = workflow.GetSignalChannel(ctx, signalPrompt)
		resumeCh    = workflow.GetSignalChannel(ctx, signalResume)
		cancelCh    = workflow.GetSignalChannel(ctx, signalCancel)
		closeCh     = workflow.GetSignalChannel(ctx, signalClose)
		childWaitCh = workflow.GetSignalChannel(ctx, signalChildWaiting)
	)
	_ = workflow.SetQueryHandler(ctx, queryStatus, func() (durable.SessionStatus, error) {
		st := durable.SessionStatus{
			ID:         in.SessionID,
			Parent:     in.Parent,
			Specialist: in.Specialist,
			Kind:       "",
			State:      durable.SessionRunning,
			Waiting:    yielded,
			Result:     result,
		}
		if in.Worker != "" {
			st.Kind = durable.SessionKindWorker
			st.Specialist = in.Worker
		} else if in.Specialist != "" {
			st.Kind = durable.SessionKindSpecialist
		}
		if terminal != "" {
			st.State = terminal
			st.Waiting = false
		} else if closed {
			st.State = durable.SessionComplete
			st.Waiting = false
		}
		return st, nil
	})
	_ = workflow.SetQueryHandler(ctx, queryChildren, func() ([]durable.SessionID, error) {
		return spawnedIDs(spawned), nil
	})
	cancelSpawned := func() {
		for _, c := range spawned {
			if c.child != nil {
				var exec workflow.Execution
				if err := c.child.GetChildWorkflowExecution().Get(ctx, &exec); err == nil {
					_ = workflow.RequestCancelExternalWorkflow(ctx, exec.ID, exec.RunID).Get(ctx, nil)
				}
			}
			if c.cancel != nil {
				c.cancel()
			}
		}
		spawned = nil
		inbox = nil
		nextAgentID = ""
		nextMCP = nil
	}
	wait := func() waitSignal {
		return waitSession(ctx, promptCh, resumeCh, cancelCh, closeCh, childWaitCh)
	}

	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: resolveActivityTimeout(in.ActivityTimeout),
		HeartbeatTimeout:    resolveHeartbeatTimeout(in.HeartbeatTimeout),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: resolveActivityAttempts(in.ActivityAttempts),
		},
	}

	runWorker := func(task string) error {
		actCtx := workflow.WithActivityOptions(ctx, activityOpts)
		var out string
		err := workflow.ExecuteActivity(actCtx, "RunJob", runJobInput{Name: in.Worker, Task: task}).Get(ctx, &out)
		if err != nil {
			terminal = durable.SessionFailed
			msg := failureText(err)
			result = msg
			_ = workflow.ExecuteActivity(actCtx, "EmitEvent", emitEventInput{
				SessionID: in.SessionID,
				Event:     tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg},
			}).Get(ctx, nil)
			return err
		}
		result = out
		terminal = durable.SessionComplete
		_ = workflow.ExecuteActivity(actCtx, "EmitEvent", emitEventInput{
			SessionID: in.SessionID,
			Event:     tacklr.StreamEvent{Type: tacklr.StreamEventComplete},
		}).Get(ctx, nil)
		return nil
	}
	if in.Worker != "" && in.Prompt != "" {
		err := runWorker(in.Prompt)
		return result, err
	}

	session := &durable.Turn{
		SessionID:  in.SessionID,
		AgentID:    agentID,
		Specialist: in.Specialist,
		Worker:     in.Worker,
		Parent:     in.Parent,
		Mounts:     mounts,
		MCP:        mcpServers,
		Seed:       seed,
	}
	ex := &wfExec{
		ctx:        ctx,
		sessionCtx: ctx,
		in:         in,
		opts:       activityOpts,
		promptCh:   promptCh,
		cancelCh:   cancelCh,
		spawned:    &spawned,
	}
	ex.cancelAll = cancelSpawned
	ex.mounts = &session.Mounts
	runSlice := func(user *tacklr.Message, resume map[string][]byte, auth durable.AuthContext, kind string, extra map[string]any) {
		session.AgentID = agentID
		session.MCP = mcpServers
		session.Mounts = mounts
		session.Inbox = inbox
		session.NextAgent = nextAgentID
		session.NextMCP = nextMCP
		ex.agentID = agentID
		ex.sessionCtx, ex.pinned = openTurnLocality(ctx, in.TurnLocalityTimeout, 2*time.Second)
		n := 0
		if user != nil {
			n = len(user.Content)
		}
		logInfo(ctx, "turn start",
			"kind", kind, "agent_id", agentID, "session_id", in.SessionID,
			"prompt_len", n, "resume_count", len(resume),
		)
		_, endTurn := startTurn(ex.sessionCtx, agentID, in.SessionID, kind)
		session.Run(ex, user, resume, auth, extra)
		outcome := telemetry.OutcomeOK
		if session.Yielded {
			outcome = telemetry.OutcomeYield
		} else if errors.Is(session.Err, context.Canceled) {
			outcome = telemetry.OutcomeCancelled
		} else if session.Terminal == durable.SessionFailed {
			outcome = telemetry.OutcomeError
		}
		endTurn(outcome, session.Err)
		if ex.pinned {
			workflow.CompleteSession(ex.sessionCtx)
			ex.pinned = false
		}
		ex.sessionCtx = ctx
		if session.Yielded && in.Parent != "" {
			_ = workflow.SignalExternalWorkflow(ctx, string(in.Parent), "", signalChildWaiting, in.SessionID).Get(ctx, nil)
		}
		agentID = session.AgentID
		mcpServers = session.MCP
		mounts = session.Mounts
		inbox = session.Inbox
		nextAgentID = session.NextAgent
		nextMCP = session.NextMCP
		yielded = session.Yielded
		terminal = session.Terminal
		result = session.Result
	}

	if in.Prompt != "" {
		runSlice(&tacklr.Message{Role: tacklr.RoleUser, Content: in.Prompt}, nil, durable.AuthContext{}, telemetry.TurnKindPrompt, nil)
		for session.Yielded {
			ev := wait()
			switch ev.kind {
			case signalPrompt:
				session.Steer(ev.prompt, &session.Overlay)
			case signalResume:
				runSlice(nil, ev.resume.Responses, ev.resume.Auth, telemetry.TurnKindResume, ev.resume.State)
			case signalCancel:
				ex.CancelAll()
				terminal = durable.SessionFailed
				session.Yielded = false
				msg := context.Canceled.Error()
				ex.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
			case signalClose:
				session.Yielded = false
				closed = true
			}
		}
		if terminal == durable.SessionFailed {
			if result != "" {
				return result, errors.New(result)
			}
			return "", errors.New("child failed")
		}
		return result, nil
	}

	for !closed {
		ev := wait()
		switch ev.kind {
		case signalClose:
			closed = true
		case signalCancel:
			// Idle cancel must still stop child sessions. Do not emit a stream
			// error: that poisons the next prompt's Subscribe(after Head).
			// A parked turn does emit: the turn itself was aborted.
			if yielded {
				ex.CancelAll()
				terminal = durable.SessionFailed
				yielded = false
				session.Yielded = false
				session.Overlay = nil
				msg := context.Canceled.Error()
				ex.Emit(tacklr.StreamEvent{Type: tacklr.StreamEventError, Fail: msg, Content: msg})
				continue
			}
			cancelSpawned()
			continue
		case signalPrompt:
			if yielded {
				session.Steer(ev.prompt, &session.Overlay)
				mounts = session.Mounts
				inbox = session.Inbox
				nextAgentID = session.NextAgent
				nextMCP = session.NextMCP
				continue
			}
			if in.Worker != "" {
				user := adapter.UserFromPrompt(ev.prompt.Text, ev.prompt.UserMessage)
				task := ""
				if user != nil {
					task = user.Content
				}
				_ = runWorker(task)
				continue
			}
			if ev.prompt.AgentID != "" {
				agentID = ev.prompt.AgentID
			} else if nextAgentID != "" {
				agentID = nextAgentID
			}
			nextAgentID = ""
			if ev.prompt.MCPServers != nil {
				mcpServers = ev.prompt.MCPServers
			} else if nextMCP != nil {
				mcpServers = nextMCP
			}
			nextMCP = nil
			user := adapter.UserFromPrompt(ev.prompt.Text, ev.prompt.UserMessage)
			runSlice(user, nil, ev.prompt.Auth, telemetry.TurnKindPrompt, ev.prompt.State)
		case signalResume:
			yielded = false
			runSlice(nil, ev.resume.Responses, ev.resume.Auth, telemetry.TurnKindResume, ev.resume.State)
		}
	}
	return result, nil
}

// wfExec is the Temporal driver for durable.Turn.
type wfExec struct {
	ctx        workflow.Context
	sessionCtx workflow.Context
	pinned     bool
	in         workflowInput
	opts       workflow.ActivityOptions
	promptCh   workflow.ReceiveChannel
	cancelCh   workflow.ReceiveChannel
	spawned    *[]childRun
	mounts     *[]durable.MountRecipe
	agentID    string
	cancelAll  func()
}

func (w *wfExec) Infer(in durable.InferenceInput) (durable.InferenceOutput, error) {
	var out durable.InferenceOutput
	actCtx := workflow.WithActivityOptions(w.sessionCtx, w.opts)
	cctx, cancelAct := workflow.WithCancel(actCtx)
	fut := workflow.ExecuteActivity(cctx, "Inference", in)
	var err error
	s := workflow.NewSelector(w.ctx)
	s.AddFuture(fut, func(f workflow.Future) { err = f.Get(w.ctx, &out) })
	s.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		w.CancelAll()
		cancelAct()
		err = fut.Get(w.ctx, &out)
	})
	s.AddReceive(cctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		cancelAct()
		err = fut.Get(w.ctx, &out)
	})
	s.Select(w.ctx)
	for w.cancelCh.ReceiveAsync(nil) {
	}
	if err == nil || turnCanceled(w.ctx, err) {
		if err != nil {
			err = context.Canceled
		}
		return out, err
	}
	return out, errors.New(failureText(err))
}

func (w *wfExec) Tool(in durable.ToolInput) (durable.ToolOutput, error) {
	var out durable.ToolOutput
	actCtx := workflow.WithActivityOptions(w.sessionCtx, w.opts)
	cctx, cancelAct := workflow.WithCancel(actCtx)
	fut := workflow.ExecuteActivity(cctx, "Tool", in)
	var err error
	s := workflow.NewSelector(w.ctx)
	s.AddFuture(fut, func(f workflow.Future) { err = f.Get(w.ctx, &out) })
	s.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		w.CancelAll()
		cancelAct()
		err = fut.Get(w.ctx, &out)
	})
	s.AddReceive(cctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		cancelAct()
		err = fut.Get(w.ctx, &out)
	})
	s.Select(w.ctx)
	for w.cancelCh.ReceiveAsync(nil) {
	}
	if err == nil || turnCanceled(w.ctx, err) {
		if err != nil {
			err = context.Canceled
		}
		return out, err
	}
	return out, errors.New(failureText(err))
}

func (w *wfExec) Commit(in durable.CommitInput) error {
	actCtx := workflow.WithActivityOptions(w.sessionCtx, w.opts)
	cctx, cancelAct := workflow.WithCancel(actCtx)
	fut := workflow.ExecuteActivity(cctx, "CommitToolOutput", in)
	var err error
	s := workflow.NewSelector(w.ctx)
	s.AddFuture(fut, func(f workflow.Future) { err = f.Get(w.ctx, nil) })
	s.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		w.CancelAll()
		cancelAct()
		err = fut.Get(w.ctx, nil)
	})
	s.AddReceive(cctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		cancelAct()
		err = fut.Get(w.ctx, nil)
	})
	s.Select(w.ctx)
	for w.cancelCh.ReceiveAsync(nil) {
	}
	if err == nil || turnCanceled(w.ctx, err) {
		if err != nil {
			return context.Canceled
		}
		return nil
	}
	return errors.New(failureText(err))
}

func (w *wfExec) Emit(ev tacklr.StreamEvent) {
	actCtx := workflow.WithActivityOptions(w.sessionCtx, w.opts)
	cctx, cancelAct := workflow.WithCancel(actCtx)
	fut := workflow.ExecuteActivity(cctx, "EmitEvent", emitEventInput{SessionID: w.in.SessionID, Event: ev})
	var err error
	s := workflow.NewSelector(w.ctx)
	s.AddFuture(fut, func(f workflow.Future) { err = f.Get(w.ctx, nil) })
	s.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		w.CancelAll()
		cancelAct()
		err = fut.Get(w.ctx, nil)
	})
	s.AddReceive(cctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		cancelAct()
		err = fut.Get(w.ctx, nil)
	})
	s.Select(w.ctx)
	for w.cancelCh.ReceiveAsync(nil) {
	}
	_ = err
}

func (w *wfExec) DrainPrompts() []durable.PromptIn {
	var out []durable.PromptIn
	var p durable.PromptIn
	for w.promptCh.ReceiveAsync(&p) {
		out = append(out, p)
		p = durable.PromptIn{}
	}
	return out
}

func (w *wfExec) ChildIDs() []durable.SessionID { return spawnedIDs(*w.spawned) }

func (w *wfExec) CancelAll() {
	if w.cancelAll != nil {
		w.cancelAll()
	}
}

func (w *wfExec) ApplyChild(tout durable.ToolOutput) error {
	mounts := []durable.MountRecipe(nil)
	if w.mounts != nil {
		mounts = *w.mounts
	}
	return applyChildIntent(w.ctx, w.sessionCtx, w.spawned, tout, w.in, w.agentID, mounts)
}

func (w *wfExec) AwaitChild(id durable.SessionID) (string, error) {
	out, err := waitChildTool(w.ctx, w.spawned, id, w.cancelCh, w.CancelAll)
	if err != nil && (turnCanceled(w.ctx, err) || temporal.IsCanceledError(err)) {
		return "", context.Canceled
	}
	return out, err
}

func (w *wfExec) Harvest(inbox *[]*tacklr.Message) {
	harvestReadyChildren(w.ctx, w.spawned, inbox)
}

func (w *wfExec) WaitJobs(t *durable.Turn, state *map[string]any) error {
	ctx := w.ctx
	harvestReadyChildren(ctx, w.spawned, &t.Inbox)
	if len(t.Inbox) > 0 || len(*w.spawned) == 0 {
		return nil
	}
	var ret error
	s := workflow.NewSelector(ctx)
	for _, c := range *w.spawned {
		if c.done {
			continue
		}
		id := c.id
		s.AddFuture(c.fut, func(f workflow.Future) {
			var result string
			err := f.Get(ctx, &result)
			if msg := markChildDone(w.spawned, id, result, err); msg != nil {
				t.Inbox = append(t.Inbox, msg)
			}
		})
	}
	s.AddReceive(w.promptCh, func(c workflow.ReceiveChannel, more bool) {
		var p durable.PromptIn
		c.Receive(ctx, &p)
		t.Steer(p, state)
	})
	s.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, nil)
		ret = context.Canceled
	})
	s.Select(ctx)
	return ret
}

// waitSession is the idle/park demux. Temporal signal channels are named
// mailboxes (not opened/closed). ChildWaiting is drained so the mailbox
// does not back up.
func waitSession(
	ctx workflow.Context,
	promptCh, resumeCh, cancelCh, closeCh, childWaitCh workflow.ReceiveChannel,
) waitSignal {
	for {
		var out waitSignal
		s := workflow.NewSelector(ctx)
		s.AddReceive(promptCh, func(c workflow.ReceiveChannel, more bool) {
			var p durable.PromptIn
			c.Receive(ctx, &p)
			out.kind, out.prompt = signalPrompt, p
		})
		s.AddReceive(resumeCh, func(c workflow.ReceiveChannel, more bool) {
			var p durable.ResumeIn
			c.Receive(ctx, &p)
			out.kind, out.resume = signalResume, p
		})
		s.AddReceive(cancelCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			out.kind = signalCancel
		})
		s.AddReceive(closeCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			out.kind = signalClose
		})
		s.AddReceive(childWaitCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
		})
		s.Select(ctx)
		if out.kind != "" {
			return out
		}
	}
}

// failureText reads ApplicationError.Message. Error() appends the cause,
// which repeats the same text.
func failureText(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		if msg := app.Message(); msg != "" {
			return msg
		}
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

func turnCanceled(ctx workflow.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	return temporal.IsCanceledError(err) || errors.Is(err, workflow.ErrCanceled) || errors.Is(err, workflow.ErrSessionFailed)
}

// openTurnLocality pins activities to one worker when the host set a timeout.
// Timeout <= 0 skips CreateSession (no hidden default).
func openTurnLocality(ctx workflow.Context, timeout, creation time.Duration) (workflow.Context, bool) {
	if timeout <= 0 {
		return ctx, false
	}
	if creation <= 0 {
		creation = 2 * time.Second
	}
	sctx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
		CreationTimeout:  creation,
		ExecutionTimeout: timeout,
	})
	if err != nil {
		workflow.GetLogger(ctx).Error("worker session", "error", err)
		return ctx, false
	}
	return sctx, true
}
