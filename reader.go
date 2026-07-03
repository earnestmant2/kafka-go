package kafka

import (
	"context"
	"sync"
)

// ... existing code ...

func (r *Reader) run(ctx context.Context) {
	// ... existing loop ...
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.rebalanceTrigger:
			// Clear buffer on rebalance
			r.mu.Lock()
			r.buffer = nil
			r.mu.Unlock()
		default:
			// ... fetch logic ...
		}
	}
}

func (r *Reader) commitMessages(ctx context.Context, msgs ...Message) error {
	err := r.client.CommitMessages(ctx, msgs...)
	if err != nil {
		// Invalidate state on commit failure
		r.mu.Lock()
		r.lastCommittedOffset = -1
		r.mu.Unlock()
		return err
	}
	return nil
}