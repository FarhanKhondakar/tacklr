package adapter

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/log"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// CloseTurnVFS unmounts a turn-scoped MountSession (FUSE telemetry, Close, host dir).
func CloseTurnVFS(ms *vfs.MountSession) {
	if ms == nil {
		return
	}
	dir := ms.HostDir()
	if dir != "" {
		telemetry.EmitEvent(context.Background(), telemetry.EventFuseUnmount)
	}
	_ = ms.Close()
	if dir != "" {
		_ = os.Remove(dir)
	}
}

// CloseTurnTrees closes the agent workspace tree and the host-only skills tree.
func CloseTurnTrees(workspace, skills *vfs.MountSession) {
	CloseTurnVFS(workspace)
	CloseTurnVFS(skills)
}

// OpenSkillsVFS builds the host-only skills MountSession from AgentOptions.OpenSkills.
// It does not attach a FUSE projection. The agent never receives this session.
func OpenSkillsVFS(ctx context.Context, threadID string, agent tacklr.AgentOptions) (*vfs.MountSession, error) {
	if agent.OpenSkills == nil {
		return nil, nil
	}
	return agent.OpenSkills(ctx, threadID, vfs.Request{})
}

// OpenTurnSessions opens the agent workspace and the host-only skills tree.
// On OpenSkills failure the workspace session is closed.
func OpenTurnSessions(ctx context.Context, threadID string, agent tacklr.AgentOptions, bindings []vfs.Binding, proj vfs.Projection) (workspace, skills *vfs.MountSession, err error) {
	workspace, err = OpenTurnVFS(ctx, threadID, agent, bindings, proj)
	if err != nil {
		return nil, nil, err
	}
	skills, err = OpenSkillsVFS(ctx, threadID, agent)
	if err != nil {
		CloseTurnVFS(workspace)
		return nil, nil, err
	}
	return workspace, skills, nil
}

// OpenTurnVFS builds the turn-scoped MountSession from AgentOptions.OpenVFS.
// Nil when OpenVFS is nil or the projection is unavailable.
func OpenTurnVFS(ctx context.Context, threadID string, agent tacklr.AgentOptions, bindings []vfs.Binding, proj vfs.Projection) (*vfs.MountSession, error) {
	if agent.OpenVFS == nil {
		return nil, nil
	}
	if proj == nil || !proj.Available() {
		telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeUnavailable)
		return nil, nil
	}
	ms, err := agent.OpenVFS(ctx, threadID, vfs.Request{Bindings: bindings})
	if err != nil {
		return nil, err
	}
	if ms.HostDir() == "" {
		if err := proj.Attach(ms, threadID); err != nil {
			telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeError)
			CloseTurnVFS(ms)
			return nil, err
		}
		telemetry.EmitEvent(ctx, telemetry.EventFuseMount,
			log.String(telemetry.AttrSessionID, threadID),
		)
		telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeOK)
	}
	return ms, nil
}
