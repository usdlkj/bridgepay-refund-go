package report

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"bridgepay-refund-go/internal/storage"
	"github.com/xuri/excelize/v2"
)

type Store interface {
	ListReports(context.Context, []storage.ReportFilter) ([]storage.Report, error)
	CreateReport(context.Context, storage.ReportType, string, time.Time, time.Time) (storage.Report, error)
	ScheduledReportExists(context.Context, storage.ReportType, time.Time) (bool, error)
	GetReport(context.Context, string) (storage.Report, error)
	PendingReports(context.Context) ([]storage.Report, error)
	CompleteReport(context.Context, string, json.RawMessage) error
	ListRefundsForReport(context.Context, time.Time, time.Time) ([]storage.RefundAdminRecord, error)
	ListIlumaLogsForReport(context.Context, time.Time, time.Time) ([]storage.IlumaCallLog, error)
	WithAdvisoryLock(context.Context, string, func(context.Context) error) (bool, error)
}

type Starter func(string, func(context.Context) error) bool

type Service struct {
	store Store
	start Starter
	zone  *time.Location
}

type CreateRequest struct {
	Date string             `json:"date"`
	Type storage.ReportType `json:"type"`
}

type Record struct {
	ID              string               `json:"id"`
	Name            *string              `json:"name"`
	RefundStartDate *time.Time           `json:"refundStartDate"`
	RefundEndDate   *time.Time           `json:"refundEndDate"`
	ReportData      json.RawMessage      `json:"reportData"`
	ReportType      storage.ReportType   `json:"reportType"`
	ReportStatus    storage.ReportStatus `json:"reportStatus"`
	CreatedAt       time.Time            `json:"createdAt"`
	UpdatedAt       time.Time            `json:"updatedAt"`
	DeletedAt       *time.Time           `json:"deletedAt,omitempty"`
}

func New(store Store, start Starter) *Service {
	zone, _ := time.LoadLocation("Asia/Jakarta")
	if start == nil {
		start = func(_ string, job func(context.Context) error) bool {
			go func() { _ = job(context.Background()) }()
			return true
		}
	}
	return &Service{store: store, start: start, zone: zone}
}

func (s *Service) List(ctx context.Context, filters []storage.ReportFilter) ([]Record, error) {
	reports, err := s.store.ListReports(ctx, filters)
	if err != nil {
		return nil, err
	}
	result := make([]Record, 0, len(reports))
	for _, item := range reports {
		result = append(result, mapRecord(item))
	}
	return result, nil
}

func (s *Service) Create(ctx context.Context, request CreateRequest) (Record, error) {
	day, err := time.ParseInLocation("2006-01-02", request.Date, s.zone)
	if err != nil {
		return Record{}, errors.New("date must use YYYY-MM-DD")
	}
	if request.Type != storage.ReportRefund && request.Type != storage.ReportIluma {
		return Record{}, errors.New("type must be refund or iluma")
	}
	created, err := s.store.CreateReport(ctx, request.Type, string(request.Type)+day.Format("02012006"), day, day)
	if err != nil {
		return Record{}, err
	}
	s.start("report-generation", func(jobCtx context.Context) error { return s.Generate(jobCtx, created) })
	return mapRecord(created), nil
}

func (s *Service) CreateScheduled(ctx context.Context, reportType storage.ReportType, day time.Time) error {
	key := "bridgepay-refund-go:report:" + string(reportType) + ":" + day.In(s.zone).Format("2006-01-02")
	_, err := s.store.WithAdvisoryLock(ctx, key, func(lockCtx context.Context) error {
		exists, err := s.store.ScheduledReportExists(lockCtx, reportType, day)
		if err != nil || exists {
			return err
		}
		_, err = s.Create(lockCtx, CreateRequest{Date: day.In(s.zone).Format("2006-01-02"), Type: reportType})
		return err
	})
	return err
}

func (s *Service) Resume(ctx context.Context) error {
	pending, err := s.store.PendingReports(ctx)
	if err != nil {
		return err
	}
	for _, item := range pending {
		report := item
		s.start("report-generation", func(jobCtx context.Context) error { return s.Generate(jobCtx, report) })
	}
	return nil
}

func (s *Service) Generate(ctx context.Context, item storage.Report) error {
	if item.RefundStart == nil {
		return errors.New("report start date is missing")
	}
	start, end := reportRange(*item.RefundStart, s.zone)
	var rows []map[string]any
	var err error
	if item.Type == storage.ReportRefund {
		rows, err = s.refundRows(ctx, start, end)
	} else {
		rows, err = s.ilumaRows(ctx, start, end)
	}
	if err != nil {
		return err
	}
	data, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	return s.store.CompleteReport(ctx, item.ID, data)
}

func (s *Service) Download(ctx context.Context, id string) (string, storage.ReportType, []byte, error) {
	item, err := s.store.GetReport(ctx, id)
	if err != nil {
		return "", "", nil, err
	}
	var rows []map[string]any
	if len(item.Data) > 0 && string(item.Data) != "null" {
		if err := json.Unmarshal(item.Data, &rows); err != nil {
			return "", "", nil, err
		}
	}
	name := "report"
	if item.Name != nil && *item.Name != "" {
		name = strings.TrimSuffix(*item.Name, ".txt")
	}
	contents, err := workbook(name, item.Type, rows)
	return name, item.Type, contents, err
}

func (s *Service) refundRows(ctx context.Context, start, end time.Time) ([]map[string]any, error) {
	refunds, err := s.store.ListRefundsForReport(ctx, start, end)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(refunds))
	for index, item := range refunds {
		r := item.Refund
		var amount struct {
			Fee, Tax, TotalAmount any
		}
		var bank struct {
			BankCode, BankNumber, BankName string
		}
		_ = json.Unmarshal(r.AmountData, &amount)
		_ = json.Unmarshal(r.BankData, &bank)
		refundID := value(r.RefundGANumber)
		created := time.Now()
		rows = append(rows, map[string]any{
			"seqNo": index + 1, "refundDate": r.CreatedAt.In(s.zone).Format("02012006"),
			"cancelTime": r.CreatedAt.In(s.zone).Format("02/01/2006 15:04:05"), "refundType": "-",
			"refundPerson": "-", "refundCharge": amount.Fee, "refundChargeTax": amount.Tax,
			"refundAmount": numberValue(r.Amount), "refundTrandeNo": refundID, "PlatTradeNo": "-",
			"refundBankCode": bank.BankCode, "refundBankName": "-", "refundAccount": bank.BankNumber,
			"refundAccountName": bank.BankName, "ActualRefundAmount": amount.TotalAmount,
			"PassengerName": "-", "encryptedIdNumber": "-", "nationality": "-", "orderNumber": "-",
			"ticketNo": "-", "ticketingStation": "-", "businessArea": "-", "officeNo": "-",
			"windowNo": "-", "shiftNo": "-", "operatorName": "-", "ticketingTime": "-",
			"departureTime": "-", "trainNo": "-", "origin": "-", "carsNumber": "-", "seatNumber": "-",
			"originCode": "-", "purchaseDate": "-", "destination": "-", "destinationCode": "-",
			"arrivalTime": "-", "seatClass": "-", "ticketType": "-", "originalTicketPrice": "-",
			"createdAt": created, "updatedAt": created,
		})
	}
	return rows, nil
}

func (s *Service) ilumaRows(ctx context.Context, start, end time.Time) ([]map[string]any, error) {
	logs, err := s.store.ListIlumaLogsForReport(ctx, start, end)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(logs))
	for _, log := range logs {
		rows = append(rows, map[string]any{"endPoint": log.URL, "method": log.Method,
			"callData": log.CreatedAt.In(s.zone).Format("02/01/2006 15:04:05")})
	}
	return rows, nil
}

func mapRecord(item storage.Report) Record {
	data := item.Data
	if len(data) == 0 {
		data = json.RawMessage("null")
	}
	return Record{ID: item.ID, Name: item.Name, RefundStartDate: item.RefundStart,
		RefundEndDate: item.RefundEnd, ReportData: data, ReportType: item.Type,
		ReportStatus: item.Status, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt, DeletedAt: item.DeletedAt}
}

func reportRange(day time.Time, zone *time.Location) (time.Time, time.Time) {
	year, month, date := day.Date()
	start := time.Date(year, month, date, 0, 0, 0, 0, zone)
	return start, start.AddDate(0, 0, 1).Add(-time.Nanosecond)
}

func value(raw *string) any {
	if raw == nil {
		return nil
	}
	return *raw
}

func numberValue(raw *string) any {
	if raw == nil {
		return nil
	}
	return json.Number(*raw)
}

var refundHeaders = []string{"seqNo", "refundDate", "cancelTime", "refundType", "refundPerson", "refundCharge",
	"refundChargeTax", "refundAmount", "refundTrandeNo", "PlatTradeNo", "refundBankCode", "refundBankName",
	"refundAccount", "refundAccountName", "ActualRefundAmount", "PassengerName", "encryptedIdNumber", "nationality",
	"orderNumber", "ticketNo", "ticketingStation", "businessArea", "officeNo", "windowNo", "shiftNo", "operatorName",
	"ticketingTime", "departureTime", "trainNo", "origin", "carsNumber", "seatNumber", "originCode", "purchaseDate",
	"destination", "destinationCode", "arrivalTime", "seatClass", "ticketType", "originalTicketPrice"}

func workbook(title string, reportType storage.ReportType, rows []map[string]any) ([]byte, error) {
	file := excelize.NewFile()
	defer func() { _ = file.Close() }()
	sheet := "Sheet1"
	if err := file.SetSheetName(sheet, title); err != nil {
		return nil, err
	}
	sheet = title
	headers := refundHeaders
	if reportType != storage.ReportRefund {
		headers = []string{"endPoint", "method", "callData"}
	}
	headerValues := make([]any, len(headers))
	for i, header := range headers {
		headerValues[i] = header
	}
	if err := file.SetSheetRow(sheet, "A1", &headerValues); err != nil {
		return nil, err
	}
	for rowIndex, row := range rows {
		values := make([]any, len(headers))
		for column, key := range headers {
			values[column] = row[key]
		}
		axis := "A" + strconv.Itoa(rowIndex+2)
		if err := file.SetSheetRow(sheet, axis, &values); err != nil {
			return nil, err
		}
	}
	var output bytes.Buffer
	if err := file.Write(&output); err != nil {
		return nil, err
	}
	return io.ReadAll(&output)
}
