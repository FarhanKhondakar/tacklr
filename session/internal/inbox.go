package adapter

import (
	"context"

	"github.com/ryanaldo34/tacklr"
)

// AppendMessages is the unlocked FIFO append. Temporal wait-loop only.
func AppendMessages(box []*tacklr.Message, msgs ...*tacklr.Message) []*tacklr.Message {
	for _, m := range msgs {
		if m != nil {
			box = append(box, m)
		}
	}
	return box
}

// AbsorbAll writes msgs into the window in order. consumed is how many
// absorb calls ran (including a failed one). Absorb failure is terminal
// for the batch: leftover messages are not put back.
func AbsorbAll(ctx context.Context, absorb func(context.Context, *tacklr.Message, chan tacklr.StreamEvent) error, msgs []*tacklr.Message, out chan tacklr.StreamEvent) (consumed int, err error) {
	for i, msg := range msgs {
		if err := absorb(ctx, msg, out); err != nil {
			return i + 1, err
		}
	}
	return len(msgs), nil
}
