package server

import "sync"

// notifier wakes a worker's long-poll the moment a job is assigned to it,
// so submit-to-start latency does not depend on the poll interval.
type notifier struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

func newNotifier() *notifier {
	return &notifier{waiters: map[string][]chan struct{}{}}
}

func (n *notifier) wait(workerID string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.waiters[workerID] = append(n.waiters[workerID], ch)
	n.mu.Unlock()

	return ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		out := n.waiters[workerID][:0]
		for _, c := range n.waiters[workerID] {
			if c != ch {
				out = append(out, c)
			}
		}
		if len(out) == 0 {
			delete(n.waiters, workerID)
		} else {
			n.waiters[workerID] = out
		}
	}
}

func (n *notifier) poke(workerID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ch := range n.waiters[workerID] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
