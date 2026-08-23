package temporal

import (
	"context"
	"sync"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/ryanaldo34/tacklr/durable"
)

// eventBridge is the in-process live-event registry that makes hybrid
// live + replay streaming possible when the Executor and the WorkerHost share
// a process (the recommended deployment). RunStepActivity installs an event
// sink on the step context that forwards each produced harness event here; a
// RunHandle.Events consumer subscribes, replays history up to the join point,
// then forwards live events. A worker deployed separately degrades to pure
// history replay because no bridge is registered.
type eventBridge struct {
	mu   sync.Mutex
	subs map[string]map[chan durable.BridgedEvent]struct{}
}

func newEventBridge() *eventBridge {
	return &eventBridge{subs: make(map[string]map[chan durable.BridgedEvent]struct{})}
}

// subscribe registers a consumer for a run's live events. It returns the
// channel and an unsubscribe func. The channel is never closed by the bridge.
func (b *eventBridge) subscribe(runID string) (chan durable.BridgedEvent, func()) {
	ch := make(chan durable.BridgedEvent, 256)
	b.mu.Lock()
	if b.subs[runID] == nil {
		b.subs[runID] = make(map[chan durable.BridgedEvent]struct{})
	}
	b.subs[runID][ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs[runID], ch)
		if len(b.subs[runID]) == 0 {
			delete(b.subs, runID)
		}
		b.mu.Unlock()
	}
}

// emit delivers an event to every subscriber of the run. Delivery is
// non-blocking: every live event is also recorded in workflow history (via the
// step outcome), so a lagging consumer recovers through Replay instead of
// stalling the step.
func (b *eventBridge) emit(runID string, ev durable.BridgedEvent) {
	b.mu.Lock()
	subs := b.subs[runID]
	dest := make([]chan durable.BridgedEvent, 0, len(subs))
	for ch := range subs {
		dest = append(dest, ch)
	}
	b.mu.Unlock()
	for _, ch := range dest {
		select {
		case ch <- ev:
		default:
		}
	}
}

// eventSeqBase returns the number of harness events already recorded in the
// run's workflow history (from completed RunStep activity outcomes). Live
// events emitted by the next step continue the sequence from this base, so the
// live stream and history replay agree on Seq across restarts.
func eventSeqBase(ctx context.Context, c client.Client, runID string) (int64, error) {
	iter := c.GetWorkflowHistory(ctx, runID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var base int64
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return 0, err
		}
		if event.GetEventType() != enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			continue
		}
		attrs := event.GetActivityTaskCompletedEventAttributes()
		if attrs == nil {
			continue
		}
		var outcome durable.StepOutcome
		if err := converter.GetDefaultDataConverter().FromPayloads(attrs.GetResult(), &outcome); err != nil {
			continue
		}
		base += int64(len(outcome.Events))
	}
	return base, nil
}

// replayOnly forwards the run's full event history, for executors without an
// in-process bridge (standalone scheduler, separately deployed worker).
func replayOnly(ctx context.Context, h *handle, ch chan durable.BridgedEvent) {
	defer close(ch)
	events, err := h.Replay(ctx, 0)
	if err != nil {
		return
	}
	for _, ev := range events {
		select {
		case <-ctx.Done():
			return
		case ch <- ev:
		}
	}
}

// bridgeAndReplay is the hybrid consumer: subscribe to the live bridge, replay
// history up to the current live base, then forward live events, dropping any
// Seq already delivered (the join point). Events emitted by a step are always
// also persisted to history, so nothing is lost when a subscriber joins late.
// The channel closes when the run reaches a terminal state (all events already
// bridged before the workflow completes).
func bridgeAndReplay(ctx context.Context, h *handle, b *eventBridge, ch chan durable.BridgedEvent) {
	defer close(ch)
	live, unsub := b.subscribe(h.id)
	defer unsub()

	cursor, err := eventSeqBase(ctx, h.client, h.id)
	if err != nil {
		cursor = 0
	}
	history, err := h.Replay(ctx, 0)
	if err == nil {
		for _, ev := range history {
			if ev.Seq > cursor {
				break // live already covers everything after the join point
			}
			select {
			case <-ctx.Done():
				return
			case ch <- ev:
			}
		}
	}

	// Detect the terminal state so the stream closes once the workflow
	// finishes; by then every event has been bridged (the step emits all its
	// events before it returns and the workflow completes).
	terminal := make(chan struct{})
	go func() {
		_, _ = h.Result(ctx)
		close(terminal)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-terminal:
			// Drain any remaining live events, then finish.
			for {
				select {
				case ev, ok := <-live:
					if !ok {
						return
					}
					if ev.Seq <= cursor {
						continue
					}
					cursor = ev.Seq
					select {
					case ch <- ev:
					default:
					}
				default:
					return
				}
			}
		case ev, ok := <-live:
			if !ok {
				return
			}
			if ev.Seq <= cursor {
				continue // already delivered via history replay
			}
			cursor = ev.Seq
			select {
			case <-ctx.Done():
				return
			case ch <- ev:
			}
		}
	}
}
