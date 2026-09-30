package storage

import (
	"encoding/json"
	"time"
)

type RefundStatus string

const (
	RefundPendingChecking     RefundStatus = "pendingChecking"
	RefundRBDApproval         RefundStatus = "rbdApproval"
	RefundFinanceApproval     RefundStatus = "financeApproval"
	RefundPendingDisbursement RefundStatus = "pendingDisbursement"
	RefundReject              RefundStatus = "reject"
	RefundSuccess             RefundStatus = "success"
	RefundFail                RefundStatus = "fail"
	RefundDone                RefundStatus = "done"
	RefundOnHold              RefundStatus = "onHold"
	RefundCancel              RefundStatus = "cancel"
	RefundRetry               RefundStatus = "retry"
)

type BankStatus string

const (
	BankEnabled  BankStatus = "enable"
	BankDisabled BankStatus = "disable"
)

type AccountStatus string

const (
	AccountPending   AccountStatus = "pending"
	AccountCompleted AccountStatus = "completed"
	AccountExpired   AccountStatus = "expired"
)

type AccountResult string

const (
	AccountResultSuccess AccountResult = "success"
	AccountResultFailed  AccountResult = "failed"
	AccountResultPending AccountResult = "pending"
)

type ReportType string

const (
	ReportRefund ReportType = "refund"
	ReportIluma  ReportType = "iluma"
)

type ReportStatus string

const (
	ReportProcessing ReportStatus = "process"
	ReportCompleted  ReportStatus = "completed"
)

type SignatureStatus string

const (
	SignatureAccepted SignatureStatus = "accepted"
	SignatureRejected SignatureStatus = "rejected"
)

type GatewayStatus string

const (
	GatewayProduction  GatewayStatus = "production"
	GatewayDevelopment GatewayStatus = "development"
	GatewayDisabled    GatewayStatus = "disable"
)

type Refund struct {
	ID                   string
	RefundGANumber       *string
	Status               *RefundStatus
	Amount               *string
	AmountData           json.RawMessage
	Data                 json.RawMessage
	Reason               *string
	RejectReason         *string
	RejectBy             *string
	ApprovalFinanceBy    *string
	ApprovalRBDBy        *string
	ApprovalFinanceAt    *time.Time
	ApprovalRBDAt        *time.Time
	RejectAt             *time.Time
	BankData             json.RawMessage
	RefundDate           *time.Time
	RequestData          json.RawMessage
	RetryAttempt         json.RawMessage
	RetryDate            *time.Time
	TargetRefundDate     *time.Time
	ExecuteData          json.RawMessage
	NotificationLog      json.RawMessage
	RefundDetailID       *string
	DisbursementID       *string
	DisbursementResponse json.RawMessage
	BankDataID           *string
	CreatedAt            time.Time
	UpdatedAt            time.Time
	DeletedAt            *time.Time
}

type RefundFilter struct {
	Column int
	Value  string
}

type RefundLogFilter struct {
	Column int
	Value  string
}

type RefundAdminRecord struct {
	Refund
	Detail       *RefundDetail
	Tickets      []RefundDetailTicket
	WebhookCalls []RefundWebhookCall
}

type RefundDetail struct {
	ID                                                           string
	RefundID, Email, PhoneNumber, Reason, RefundGANumber, Amount *string
	TicketOffice                                                 *string
	CreatedAt, UpdatedAt                                         time.Time
	DeletedAt                                                    *time.Time
}

type RefundDetailTicket struct {
	ID                                                                         string
	ArrivalStation, CarsNumber, DepartureStation, IdentityNumber, IdentityType *string
	Name, OrderNumber, PurchasePrice, SeatNumber, TicketClass, TicketNumber    *string
	RefundDetailID                                                             *string
	DepartureDate                                                              *time.Time
	CreatedAt, UpdatedAt                                                       time.Time
	DeletedAt                                                                  *time.Time
}

type RefundBank struct {
	ID, BankName          string
	XenditCode, IlumaCode *string
	XenditData, IlumaData json.RawMessage
	Status                BankStatus
	CreatedAt, UpdatedAt  time.Time
	DeletedAt             *time.Time
}

type BankData struct {
	ID                   string
	BankCode             string
	AccountNumberEnc     json.RawMessage
	AccountNumberHash    string
	RequestID            *string
	AccountStatus        AccountStatus
	AccountResult        *AccountResult
	IlumaData            json.RawMessage
	FailureCode          *string
	FailureMessage       *string
	LastCheckAt          *time.Time
	CreatedAt, UpdatedAt time.Time
	DeletedAt            *time.Time
}

type IlumaCallLog struct {
	ID, URL, Method, Function  string
	Payload, Request, Response json.RawMessage
	CreatedAt, UpdatedAt       time.Time
	DeletedAt                  *time.Time
}

type IlumaCallback struct {
	ID                          string
	CallbackType, RequestNumber *string
	Payload, Response           json.RawMessage
	ResponseAt, DeletedAt       *time.Time
	CreatedAt, UpdatedAt        time.Time
}

type RefundLog struct {
	ID, Type, Location, Detail, Message, Notes string
	CreatedAt, UpdatedAt                       time.Time
	DeletedAt                                  *time.Time
}

type RefundWebhookCall struct {
	ID, RefundReference, Source string
	Payload, Response           json.RawMessage
	ResponseStatus              *int
	RefundID                    *string
	CreatedAt, UpdatedAt        time.Time
}

type TicketingCallLog struct {
	ID, RefundNumber     string
	Payload, Response    json.RawMessage
	CreatedAt, UpdatedAt time.Time
}

type Report struct {
	ID                     string
	Name                   *string
	RefundStart, RefundEnd *time.Time
	Data                   json.RawMessage
	Type                   ReportType
	Status                 ReportStatus
	CreatedAt, UpdatedAt   time.Time
	DeletedAt              *time.Time
}

type ReportFilter struct {
	Column int
	Value  string
}

// ReportDataRow preserves the Node/XLSX field spellings through explicit
// database-oriented names rather than normalizing them during migration.
type ReportDataRow struct {
	ID                                                                       string
	SequenceNumber                                                           *int
	RefundDate, CancelTime, RefundType, RefundPerson                         *string
	RefundCharge, RefundChargeTax, RefundAmount                              *string
	RefundTradeNumber, PlatformTradeNumber, RefundBankCode, RefundBankName   *string
	RefundAccount, RefundAccountName, ActualRefundAmount, PassengerName      *string
	EncryptedIDNumber, Nationality, OrderNumber, TicketNumber                *string
	TicketingStation, BusinessArea, OfficeNumber, WindowNumber, ShiftNumber  *string
	OperatorName, TicketingTime, DepartureTime, TrainNumber, Origin          *string
	CarsNumber, SeatNumber, OriginCode, PurchaseDate, Destination            *string
	DestinationCode, ArrivalTime, SeatClass, TicketType, OriginalTicketPrice *string
	ReportID                                                                 *string
}

type Configuration struct {
	ID, Name, Value      string
	CreatedAt, UpdatedAt time.Time
}

type APILogDebug struct {
	ID, Endpoint, Payload, Signature, RawPayload string
	SignatureStatus                              SignatureStatus
	CreatedAt, UpdatedAt                         time.Time
}

// PaymentGateway is mapped because it exists in the shared Node schema, but
// Refund-Go must treat it as Core-owned and read-only.
type PaymentGateway struct {
	ID, Code, Name, Credential, CredentialEncrypted, PercentageRange string
	Status                                                           GatewayStatus
	Weight                                                           int
	CredentialEnc, CredentialIV, CredentialTag, CredentialEDK        []byte
	CredentialAlgorithm                                              string
	CredentialKMD                                                    json.RawMessage
	CreatedAt, UpdatedAt                                             time.Time
	DeletedAt                                                        *time.Time
}
