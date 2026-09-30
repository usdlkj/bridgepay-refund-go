package lifecycle

import (
	"context"
	"testing"
	"time"
)

func TestTrackerDrainsAndRejectsNewWork(t *testing.T) {
	tracker := New()
	done, ok := tracker.TryBegin("refund.create", true)
	if !ok {
		t.Fatal("first work was rejected")
	}
	snapshot := tracker.BeginDrain()
	if snapshot.InFlight != 1 || snapshot.SideEffecting != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if _, ok := tracker.TryBegin("refund.status", false); ok {
		t.Fatal("work accepted after drain began")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- tracker.Wait(ctx) }()
	done()
	if err := <-finished; err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}
