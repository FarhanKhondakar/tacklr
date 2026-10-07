package temporal

import (
	"context"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	adapter "github.com/ryanaldo34/tacklr/durable/internal"
)

// childRun is one job tracked by the parent: a child session or a RunJob activity.
type childRun struct {
	id     durable.SessionID
	spec   string
	fut    workflow.Future
	child  workflow.ChildWorkflowFuture
	cancel workflow.CancelFunc
	done   bool
	result string
	err    string
}

func startChild(ctx, sessionCtx workflow.Context, parent durable.SessionID, specialist, worker, task string, childID durable.SessionID, mounts []durable.MountRecipe, in workflowInput) (childRun, error) {
	cctx := workflow.WithChildOptions(sessionCtx, workflow.ChildWorkflowOptions{
		WorkflowID:        string(childID),
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	name := specialist
	if worker != "" {
		name = worker
	}
	fut := workflow.ExecuteChildWorkflow(cctx, SessionWorkflow, workflowInput{
		SessionID:           childID,
		Parent:              parent,
		Specialist:          specialist,
		Worker:              worker,
		Prompt:              task,
		Mounts:              mounts,
		TurnLocalityTimeout: in.TurnLocalityTimeout,
		ActivityTimeout:     in.ActivityTimeout,
		HeartbeatTimeout:    in.HeartbeatTimeout,
		ActivityAttempts:    in.ActivityAttempts,
	})
	// The child-started event has to be in this workflow's history before we
	// continue. Otherwise the parent can finish before the child is scheduled.
	var exec workflow.Execution
	if err := fut.GetChildWorkflowExecution().Get(ctx, &exec); err != nil {
		return childRun{}, err
	}
	return childRun{id: childID, spec: name, fut: fut, child: fut}, nil
}

func findChild(spawned []childRun, id durable.SessionID) int {
	return slices.IndexFunc(spawned, func(c childRun) bool { return c.id == id })
}

func dropChild(spawned *[]childRun, id durable.SessionID) {
	*spawned = slices.DeleteFunc(*spawned, func(c childRun) bool { return c.id == id })
}

func markChildDone(spawned *[]childRun, id durable.SessionID, result string, err error) *tacklr.Message {
	i := findChild(*spawned, id)
	if i < 0 {
		return nil
	}
	c := (*spawned)[i]
	dropChild(spawned, id)
	st := durable.SessionStatus{ID: c.id, Specialist: c.spec, Result: result, State: durable.SessionComplete}
	if err != nil {
		st.State = durable.SessionFailed
		st.Result = err.Error()
	}
	return adapter.ChildJobMessage(st)
}

func cancelOne(ctx workflow.Context, spawned *[]childRun, id durable.SessionID) {
	i := findChild(*spawned, id)
	if i < 0 {
		return
	}
	var exec workflow.Execution
	c := (*spawned)[i]
	if c.child != nil {
		if err := c.child.GetChildWorkflowExecution().Get(ctx, &exec); err == nil {
			_ = workflow.RequestCancelExternalWorkflow(ctx, exec.ID, exec.RunID).Get(ctx, nil)
		}
	}
	if c.cancel != nil {
		c.cancel()
	}
	dropChild(spawned, id)
}

// activityChildren is the Tool-activity JobHost. It records this call's
// schedule/cancel/wait; the workflow starts, cancels, or waits via Runtime.
type activityChildren struct {
	parent   durable.SessionID
	agent    tacklr.AgentOptions
	jobs     map[string]durable.JobHandler
	known    []durable.SessionID
	jobID    durable.SessionID
	jobName  string
	jobTask  string
	child    bool
	cancelID durable.SessionID
	awaitID  durable.SessionID
}

func (a *activityChildren) Schedule(ctx context.Context, job tacklr.JobRequest, callID string) (tacklr.Job, error) {
	if err := ctx.Err(); err != nil {
		return tacklr.Job{}, err
	}
	name, task, err := adapter.NormalizeSpawn(job.Name, job.Task)
	if err != nil {
		return tacklr.Job{}, err
	}
	session := adapter.HasSpecialist(a.agent, name)
	var id durable.SessionID
	if session {
		id = durable.ChildSessionID(a.parent, name, callID)
	} else {
		if _, ok := a.jobs[name]; !ok {
			return tacklr.Job{}, fmt.Errorf("%w: %s", tacklr.ErrNotFound, name)
		}
		id = durable.JobID(a.parent, name, callID)
	}
	if slices.Contains(a.known, id) || a.jobID == id {
		return tacklr.Job{ID: string(id), Name: name, State: tacklr.JobRunning}, nil
	}
	if task == "" {
		if session {
			return tacklr.Job{}, fmt.Errorf("task_description_and_context is required: %w", tacklr.ErrInvalid)
		}
		return tacklr.Job{}, fmt.Errorf("task is required: %w", tacklr.ErrInvalid)
	}
	a.jobID, a.jobName, a.jobTask, a.child = id, name, task, session
	return tacklr.Job{ID: string(id), Name: name, State: tacklr.JobRunning}, nil
}

func (a *activityChildren) Jobs() []tacklr.Job {
	out := make([]tacklr.Job, 0, len(a.known)+1)
	seen := map[durable.SessionID]bool{}
	for _, id := range a.known {
		seen[id] = true
		out = append(out, tacklr.Job{ID: string(id), State: tacklr.JobRunning})
	}
	if a.jobID != "" && !seen[a.jobID] {
		out = append(out, tacklr.Job{ID: string(a.jobID), Name: a.jobName, State: tacklr.JobRunning})
	}
	return out
}

func (a *activityChildren) CancelJob(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sid := durable.SessionID(id)
	if sid != a.jobID && !slices.Contains(a.known, sid) {
		return adapter.UnknownChild(id)
	}
	a.cancelID = sid
	return nil
}

func (a *activityChildren) RunSpecialist(ctx context.Context, name, task, callID string) (tacklr.SpecialistResult, error) {
	if !adapter.HasSpecialist(a.agent, name) {
		return tacklr.SpecialistResult{}, fmt.Errorf("%w: %s", tacklr.ErrNotFound, name)
	}
	job, err := a.Schedule(ctx, tacklr.JobRequest{Name: name, Task: task}, callID)
	if err != nil {
		return tacklr.SpecialistResult{}, err
	}
	a.awaitID = durable.SessionID(job.ID)
	return tacklr.SpecialistResult{WaitFor: job.ID}, nil
}
