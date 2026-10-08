package adapter

import (
	"context"
	"errors"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
)

// ConstructTurn opens VFS trees and builds a TurnManager. On NewTurnManager
// failure the trees are closed.
func ConstructTurn(ctx context.Context, agent tacklr.AgentOptions, threadID string, bindings []vfs.Binding, proj vfs.Projection, extraMCP []mcp.MCPConfig) (*tacklr.TurnManager, *vfs.MountSession, *vfs.MountSession, error) {
	ms, skills, err := OpenTurnSessions(ctx, threadID, agent, bindings, proj)
	if err != nil {
		return nil, nil, nil, err
	}
	opts := tacklr.BindTurn(agent, threadID, ms, skills)
	if len(extraMCP) > 0 {
		opts.MCPConfigs = append(append([]mcp.MCPConfig(nil), agent.MCPConfigs...), extraMCP...)
	}
	h, err := tacklr.NewTurnManager(ctx, opts)
	if err != nil {
		CloseTurnTrees(ms, skills)
		return nil, nil, nil, err
	}
	return h, ms, skills, nil
}

// RestoreTurn reloads a snapshot (if any) and applies host session state.
func RestoreTurn(ctx context.Context, store session.SnapshotStore, id session.SessionID, h *tacklr.TurnManager, state map[string]any) (session.Revision, error) {
	snap, rev, err := store.Load(ctx, id)
	switch {
	case err == nil:
		if err := h.RestoreCheckpoint(snap.Checkpoint); err != nil {
			return "", err
		}
	case errors.Is(err, session.ErrSessionNotFound):
		rev = ""
	default:
		return "", err
	}
	if err := h.ApplySessionState(state); err != nil {
		return "", err
	}
	return rev, nil
}

// AbandonTurn closes a harness and its turn-scoped trees after a failed construct.
func AbandonTurn(h *tacklr.TurnManager, workspace, skills *vfs.MountSession) {
	if h != nil {
		h.Close()
	}
	CloseTurnTrees(workspace, skills)
}
