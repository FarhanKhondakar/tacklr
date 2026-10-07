package temporal

import (
	"cmp"
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/contrib/workflowstreams"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// SessionWorkflow is the Temporal type name for the session wait loop.
// NewWorker registers it. Hosts call durable.Runtime, not this function.
func SessionWorkflow(ctx workflow.Context, in workflowInput) (string, error) {
	logger := workflow.GetLogger(ctx)
	if _, err := workflowstreams.NewWorkflowStream(ctx, nil); err != nil {
		logger.Error("workflow stream", "error", err)
	}

	var spawned []childRun
	promptCh := workflow.GetSignalChannel(ctx, signalPrompt)
	resumeCh := workflow.GetSignalChannel(ctx, signalResume)
	cancelCh := workflow.GetSignalChannel(ctx, signalCancel)
	closeCh := workflow.GetSignalChannel(ctx, signalClose)
	childWaitCh := workflow.GetSignalChannel(ctx, signalChildWaiting)

	sess := &durable.Session{
		Task: in.Prompt,
		Turn: durable.Turn{
			SessionID:  in.SessionID,
			Specialist: in.Specialist,
			Worker:     in.Worker,
			Parent:     in.Parent,
			Mounts:     durable.ApplyAuth(in.Mounts, durable.AuthContext{}),
			MCP:        in.MCPServers,
			Seed:       in.State,
		},
	}
	ex := &wfExec{
		ctx:         ctx,
		sessionCtx:  ctx,
		in:          in,
		turn:        &sess.Turn,
		opts:        activityOptions(in),
		promptCh:    promptCh,
		resumeCh:    resumeCh,
		cancelCh:    cancelCh,
		closeCh:     closeCh,
		childWaitCh: childWaitCh,
		spawned:     &spawned,
	}
	_ = workflow.SetQueryHandler(ctx, queryStatus, func() (durable.SessionStatus, error) {
		st := durable.SessionStatus{
			ID:         in.SessionID,
			Parent:     in.Parent,
			Specialist: in.Specialist,
			State:      durable.SessionRunning,
			Waiting:    sess.Turn.Yielded,
			Result:     sess.Turn.Result,
		}
		if in.Worker != "" {
			st.Kind = durable.SessionKindWorker
			st.Specialist = in.Worker
		} else if in.Specialist != "" {
			st.Kind = durable.SessionKindSpecialist
		}
		if sess.Turn.Terminal != "" {
			st.State = sess.Turn.Terminal
			st.Waiting = false
		} else if sess.Closed {
			st.State = durable.SessionComplete
			st.Waiting = false
		}
		return st, nil
	})
	_ = workflow.SetQueryHandler(ctx, queryChildren, func() ([]durable.SessionID, error) {
		out := make([]durable.SessionID, len(spawned))
		for i, c := range spawned {
			out[i] = c.id
		}
		return out, nil
	})
	return sess.Run(ex, ex, ex)
}

func activityOptions(in workflowInput) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: cmp.Or(in.ActivityTimeout, defaultActivityTimeout),
		HeartbeatTimeout:    cmp.Or(in.HeartbeatTimeout, defaultHeartbeatTimeout),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: cmp.Or(in.ActivityAttempts, defaultActivityAttempts),
		},
	}
}

// wfExec is the Temporal driver for one session. It implements durable.Step,
// durable.Signals, and durable.Jobs.
type wfExec struct {
	ctx         workflow.Context
	sessionCtx  workflow.Context
	pinned      bool
	open        bool
	end         func(string, error)
	in          workflowInput
	turn        *durable.Turn
	opts        workflow.ActivityOptions
	promptCh    workflow.ReceiveChannel
	resumeCh    workflow.ReceiveChannel
	cancelCh    workflow.ReceiveChannel
	closeCh     workflow.ReceiveChannel
	childWaitCh workflow.ReceiveChannel
	spawned     *[]childRun
}

var (
	_ durable.Step    = (*wfExec)(nil)
	_ durable.Signals = (*wfExec)(nil)
	_ durable.Jobs    = (*wfExec)(nil)
)

func (w *wfExec) Begin(kind string, promptLen, resumes int) {
	if w.open {
		return
	}
	w.open = true
	w.sessionCtx, w.pinned = openTurnLocality(w.ctx, w.in.TurnLocalityTimeout, 2*time.Second)
	logInfo(w.ctx, "turn start",
		"kind", kind, "session_id", w.in.SessionID,
		"prompt_len", promptLen, "resume_count", resumes,
	)
	_, w.end = startTurn(w.sessionCtx, "", w.in.SessionID, kind)
}

func (w *wfExec) End() {
	if !w.open {
		return
	}
	outcome := telemetry.OutcomeOK
	if w.turn.Yielded {
		outcome = telemetry.OutcomeYield
	} else if errors.Is(w.turn.Err, context.Canceled) {
		outcome = telemetry.OutcomeCancelled
	} else if w.turn.Terminal == durable.SessionFailed {
		outcome = telemetry.OutcomeError
	}
	if w.end != nil {
		w.end(outcome, w.turn.Err)
	}
	if w.pinned {
		workflow.CompleteSession(w.sessionCtx)
		w.pinned = false
	}
	w.sessionCtx = w.ctx
	w.open = false
	if w.turn.Yielded && w.in.Parent != "" {
		_ = workflow.SignalExternalWorkflow(w.ctx, string(w.in.Parent), "", signalChildWaiting, w.in.SessionID).Get(w.ctx, nil)
	}
}

func (w *wfExec) Infer(in durable.InferenceInput) (durable.InferenceOutput, error) {
	var act *activities
	var out durable.InferenceOutput
	err := w.waitActivity(act.Inference, in, &out)
	return out, err
}

func (w *wfExec) Tool(in durable.ToolInput) (durable.ToolOutput, error) {
	var act *activities
	var out durable.ToolOutput
	err := w.waitActivity(act.Tool, in, &out)
	return out, err
}

func (w *wfExec) Commit(in durable.CommitInput) error {
	var act *activities
	return w.waitActivity(act.CommitToolOutput, in, nil)
}

func (w *wfExec) Emit(ev tacklr.StreamEvent) {
	var act *activities
	// The turn already chose its outcome. A failed publish must not run the model again.
	_ = w.waitActivity(act.EmitEvent, emitEventInput{SessionID: w.in.SessionID, Event: ev}, nil)
}

func (w *wfExec) Job(name, task string) (string, error) {
	var act *activities
	var out string
	err := w.waitActivity(act.RunJob, runJobInput{Name: name, Task: task}, &out)
	return out, err
}

func (w *wfExec) waitActivity(activity any, in, out any) error {
	actCtx := workflow.WithActivityOptions(w.sessionCtx, w.opts)
	cctx, cancelAct := workflow.WithCancel(actCtx)
	defer cancelAct()
	fut := workflow.ExecuteActivity(cctx, activity, in)
	var err error
	sel := workflow.NewSelector(w.ctx)
	sel.AddFuture(fut, func(f workflow.Future) { err = f.Get(w.ctx, out) })
	sel.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		cancelAct()
		err = fut.Get(w.ctx, out)
	})
	sel.AddReceive(cctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		c.Receive(w.ctx, nil)
		cancelAct()
		err = fut.Get(w.ctx, out)
	})
	sel.Select(w.ctx)
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

func (w *wfExec) Prompts() []durable.PromptIn {
	var out []durable.PromptIn
	var p durable.PromptIn
	for w.promptCh.ReceiveAsync(&p) {
		out = append(out, p)
		p = durable.PromptIn{}
	}
	return out
}

func (w *wfExec) Ready() []*tacklr.Message {
	var inbox []*tacklr.Message
	ctx := w.ctx
	for {
		if len(*w.spawned) == 0 {
			return inbox
		}
		var gotID durable.SessionID
		var gotResult string
		var gotErr error
		ready := false
		pending := 0
		sel := workflow.NewSelector(ctx)
		for _, c := range *w.spawned {
			if c.done {
				continue
			}
			pending++
			id := c.id
			sel.AddFuture(c.fut, func(f workflow.Future) {
				var result string
				err := f.Get(ctx, &result)
				gotID, gotResult, gotErr, ready = id, result, err, true
			})
		}
		if pending == 0 {
			return inbox
		}
		sel.AddDefault(func() {})
		sel.Select(ctx)
		if !ready {
			return inbox
		}
		if msg := markChildDone(w.spawned, gotID, gotResult, gotErr); msg != nil {
			inbox = adapter.AppendMessages(inbox, msg)
		}
	}
}

func (w *wfExec) Recv() durable.Wake {
	ctx := w.ctx
	for {
		var out durable.Wake
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(w.promptCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &out.Prompt)
			out.Kind = durable.WakePrompt
		})
		sel.AddReceive(w.resumeCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, &out.Resume)
			out.Kind = durable.WakeResume
		})
		sel.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			out.Kind = durable.WakeCancel
		})
		sel.AddReceive(w.closeCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			out.Kind = durable.WakeClose
		})
		sel.AddReceive(w.childWaitCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
		})
		sel.Select(ctx)
		if out.Kind != "" {
			return out
		}
	}
}

func (w *wfExec) Wait() durable.Wake {
	ctx := w.ctx
	var wake durable.Wake
	sel := workflow.NewSelector(ctx)
	for _, c := range *w.spawned {
		if c.done {
			continue
		}
		id := c.id
		sel.AddFuture(c.fut, func(f workflow.Future) {
			var result string
			err := f.Get(ctx, &result)
			if msg := markChildDone(w.spawned, id, result, err); msg != nil {
				wake = durable.Wake{Kind: durable.WakeChild, Child: msg}
			}
		})
	}
	sel.AddReceive(w.promptCh, func(c workflow.ReceiveChannel, more bool) {
		var p durable.PromptIn
		c.Receive(ctx, &p)
		wake = durable.Wake{Kind: durable.WakePrompt, Prompt: p}
	})
	sel.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, nil)
		wake = durable.Wake{Kind: durable.WakeCancel}
	})
	sel.Select(ctx)
	return wake
}

func (w *wfExec) IDs() []durable.SessionID {
	out := make([]durable.SessionID, len(*w.spawned))
	for i, c := range *w.spawned {
		out[i] = c.id
	}
	return out
}

func (w *wfExec) Start(tout durable.ToolOutput) error {
	if tout.CancelID != "" {
		cancelOne(w.ctx, w.spawned, tout.CancelID)
	}
	if tout.JobID == "" || tout.JobName == "" || findChild(*w.spawned, tout.JobID) >= 0 {
		return nil
	}
	spec, worker := "", ""
	if tout.Child {
		spec = tout.JobName
	} else {
		worker = tout.JobName
	}
	c, err := startChild(w.ctx, w.sessionCtx, w.in.SessionID, spec, worker, tout.JobTask, tout.JobID, w.turn.Mounts, w.in)
	if err != nil {
		return err
	}
	*w.spawned = append(*w.spawned, c)
	return nil
}

func (w *wfExec) Await(id durable.SessionID) (string, error) {
	ctx := w.ctx
	for {
		i := findChild(*w.spawned, id)
		if i < 0 {
			return "", durable.ErrSessionNotFound
		}
		if (*w.spawned)[i].done {
			break
		}
		cancelled := false
		sel := workflow.NewSelector(ctx)
		sel.AddFuture((*w.spawned)[i].fut, func(f workflow.Future) {
			var result string
			err := f.Get(ctx, &result)
			j := findChild(*w.spawned, id)
			if j < 0 {
				return
			}
			(*w.spawned)[j].done = true
			(*w.spawned)[j].result = result
			if err != nil {
				(*w.spawned)[j].err = err.Error()
			}
		})
		sel.AddReceive(w.cancelCh, func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			w.CancelAll()
			cancelled = true
		})
		sel.Select(ctx)
		if cancelled {
			return "", context.Canceled
		}
	}
	i := findChild(*w.spawned, id)
	if i < 0 {
		return "", durable.ErrSessionNotFound
	}
	c := (*w.spawned)[i]
	dropChild(w.spawned, id)
	if c.err != "" {
		return c.err, nil
	}
	return c.result, nil
}

func (w *wfExec) CancelAll() {
	for _, c := range *w.spawned {
		if c.child != nil {
			var exec workflow.Execution
			if err := c.child.GetChildWorkflowExecution().Get(w.ctx, &exec); err == nil {
				_ = workflow.RequestCancelExternalWorkflow(w.ctx, exec.ID, exec.RunID).Get(w.ctx, nil)
			}
		}
		if c.cancel != nil {
			c.cancel()
		}
	}
	*w.spawned = nil
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
	if timeout == 0 {
		return ctx, false
	}
	creation = cmp.Or(creation, 2*time.Second)
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
