package report

import (
	"context"
	"log/slog"
	"time"

	"bridgepay-refund-go/internal/storage"
)

type Scheduler struct {
	service *Service
	logger  *slog.Logger
	now     func() time.Time
}

func NewScheduler(service *Service, logger *slog.Logger) *Scheduler {
	return &Scheduler{service: service, logger: logger, now: time.Now}
}

func (s *Scheduler) Run(ctx context.Context) {
	go s.runJob(ctx, 4, storage.ReportRefund)
	go s.runJob(ctx, 5, storage.ReportIluma)
}

func (s *Scheduler) runJob(ctx context.Context, hour int, reportType storage.ReportType) {
	for {
		now := s.now().In(s.service.zone)
		next := nextRun(now, hour)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			day := next.AddDate(0, 0, -1)
			if err := s.service.CreateScheduled(ctx, reportType, day); err != nil {
				s.logger.ErrorContext(ctx, "scheduled report failed", "type", reportType, "date", day.Format("2006-01-02"), "error", err)
			}
		}
	}
}

func nextRun(now time.Time, hour int) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, now.Location())
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}
