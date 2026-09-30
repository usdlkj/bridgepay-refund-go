package lifecycle

import (
	"context"
	"sync"
)

type Snapshot struct {
	Accepting     bool           `json:"accepting"`
	InFlight      int            `json:"inFlight"`
	SideEffecting int            `json:"sideEffecting"`
	ByKind        map[string]int `json:"byKind"`
}

type Tracker struct {
	mu            sync.Mutex
	accepting     bool
	inFlight      int
	sideEffecting int
	byKind        map[string]int
	drained       chan struct{}
	drainedClosed bool
}

func New() *Tracker {
	return &Tracker{accepting: true, byKind: make(map[string]int), drained: make(chan struct{})}
}

func (t *Tracker) TryBegin(kind string, sideEffecting bool) (func(), bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.accepting {
		return func() {}, false
	}
	t.inFlight++
	t.byKind[kind]++
	if sideEffecting {
		t.sideEffecting++
	}
	var once sync.Once
	return func() {
		once.Do(func() { t.finish(kind, sideEffecting) })
	}, true
}

func (t *Tracker) BeginDrain() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.accepting = false
	t.closeIfDrainedLocked()
	return t.snapshotLocked()
}

func (t *Tracker) Wait(ctx context.Context) error {
	t.mu.Lock()
	if !t.accepting && t.inFlight == 0 {
		t.closeIfDrainedLocked()
	}
	drained := t.drained
	t.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked()
}

func (t *Tracker) finish(kind string, sideEffecting bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlight > 0 {
		t.inFlight--
	}
	if t.byKind[kind] <= 1 {
		delete(t.byKind, kind)
	} else {
		t.byKind[kind]--
	}
	if sideEffecting && t.sideEffecting > 0 {
		t.sideEffecting--
	}
	t.closeIfDrainedLocked()
}

func (t *Tracker) closeIfDrainedLocked() {
	if !t.accepting && t.inFlight == 0 && !t.drainedClosed {
		close(t.drained)
		t.drainedClosed = true
	}
}

func (t *Tracker) snapshotLocked() Snapshot {
	byKind := make(map[string]int, len(t.byKind))
	for kind, count := range t.byKind {
		byKind[kind] = count
	}
	return Snapshot{Accepting: t.accepting, InFlight: t.inFlight, SideEffecting: t.sideEffecting, ByKind: byKind}
}
