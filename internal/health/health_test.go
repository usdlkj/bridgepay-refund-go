package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadinessReportsAllChecksWithoutExposingErrors(t *testing.T) {
	service := New(time.Second,
		Check{Name: "up", Run: func(context.Context) error { return nil }},
		Check{Name: "down", Run: func(context.Context) error { return errors.New("postgres://secret@host/db") }},
	)
	report := service.Readiness(context.Background())
	if report.Status != "error" {
		t.Fatalf("status = %q", report.Status)
	}
	if len(report.Checks) != 2 || report.Checks[1].Status != StatusDown {
		t.Fatalf("unexpected checks: %+v", report.Checks)
	}
	if report.Checks[1].err == nil {
		t.Fatal("internal error was not retained")
	}
}

func TestFailedNamesAreStable(t *testing.T) {
	report := Report{Checks: []Result{{Name: "z", Status: StatusDown}, {Name: "a", Status: StatusDown}}}
	names := report.FailedNames()
	if names[0] != "a" || names[1] != "z" {
		t.Fatalf("FailedNames() = %v", names)
	}
}
