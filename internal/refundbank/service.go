package refundbank

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"bridgepay-refund-go/internal/coreclient"
	"bridgepay-refund-go/internal/iluma"
	"bridgepay-refund-go/internal/storage"
)

type Store interface {
	ListEnabledRefundBanks(context.Context) ([]storage.RefundBank, error)
	ListRefundBanks(context.Context, []storage.BankFilter) ([]storage.RefundBank, error)
	GetRefundBankByID(context.Context, string) (storage.RefundBank, error)
	UpdateRefundBank(context.Context, string, *storage.BankStatus, **time.Time) (storage.RefundBank, error)
	UpsertXenditRefundBank(context.Context, string, string, json.RawMessage) (storage.RefundBank, error)
	UpdateIlumaRefundBankByName(context.Context, string, string, json.RawMessage) (bool, error)
	InsertIlumaCallLog(context.Context, storage.IlumaCallLog) error
}

type CredentialProvider interface {
	XenditCredential(context.Context, string) (coreclient.XenditCredential, error)
}

type IlumaBankProvider interface {
	BankList(context.Context) iluma.HTTPResult
}

type XenditChannelProvider interface {
	PayoutChannels(context.Context, string) ([]Channel, error)
}

type Service struct {
	store       Store
	credentials CredentialProvider
	xendit      XenditChannelProvider
	iluma       IlumaBankProvider
	environment string
	logger      *slog.Logger
}

type Channel struct {
	Code string
	Name string
	Data json.RawMessage
}

type PublicBank struct {
	Code *string `json:"code"`
	Name string  `json:"name"`
}

type PublicListResponse struct {
	RetCode int          `json:"retCode"`
	RetData []PublicBank `json:"retData"`
	RetMsg  string       `json:"retMsg"`
}

type Record struct {
	ID         string          `json:"id"`
	BankName   string          `json:"bankName"`
	XenditCode *string         `json:"xenditCode"`
	XenditData json.RawMessage `json:"xenditData"`
	IlumaCode  *string         `json:"ilumaCode"`
	IlumaData  json.RawMessage `json:"ilumaData"`
	BankStatus string          `json:"bankStatus"`
	CreatedAt  string          `json:"createdAt"`
	UpdatedAt  string          `json:"updatedAt"`
	DeletedAt  *string         `json:"deletedAt"`
}

func New(store Store, credentials CredentialProvider, xendit XenditChannelProvider, ilumaProvider IlumaBankProvider, environment string, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{store: store, credentials: credentials, xendit: xendit, iluma: ilumaProvider, environment: environment, logger: logger}
}

func (s *Service) PublicList(ctx context.Context) (PublicListResponse, error) {
	banks, err := s.store.ListEnabledRefundBanks(ctx)
	if err != nil {
		return PublicListResponse{}, err
	}
	data := make([]PublicBank, 0, len(banks))
	for _, bank := range banks {
		data = append(data, PublicBank{Code: bank.XenditCode, Name: bank.BankName})
	}
	return PublicListResponse{RetCode: 0, RetData: data, RetMsg: "success"}, nil
}

func (s *Service) List(ctx context.Context, filters []storage.BankFilter) ([]Record, error) {
	banks, err := s.store.ListRefundBanks(ctx, filters)
	if err != nil {
		return nil, err
	}
	return records(banks), nil
}

func (s *Service) View(ctx context.Context, id string) (Record, error) {
	bank, err := s.store.GetRefundBankByID(ctx, id)
	if err != nil {
		return Record{}, err
	}
	return record(bank), nil
}

func (s *Service) Update(ctx context.Context, id string, status *storage.BankStatus, deletedAt **time.Time) (Record, error) {
	if status != nil && *status != storage.BankEnabled && *status != storage.BankDisabled {
		return Record{}, errors.New("bankStatus must be enable or disable")
	}
	bank, err := s.store.UpdateRefundBank(ctx, id, status, deletedAt)
	if err != nil {
		return Record{}, err
	}
	return record(bank), nil
}

func (s *Service) Sync(ctx context.Context) error {
	credential, err := s.credentials.XenditCredential(ctx, s.environment)
	if err != nil {
		return fmt.Errorf("get Xendit credential: %w", err)
	}
	channels, err := s.xendit.PayoutChannels(ctx, credential.SecretKey)
	if err != nil {
		return err
	}
	for _, channel := range channels {
		if _, err := s.store.UpsertXenditRefundBank(ctx, strings.TrimSpace(channel.Name), strings.TrimSpace(channel.Code), channel.Data); err != nil {
			// Node's bankUpsert converts individual failures into a return value
			// that xenditSync ignores, so synchronization continues.
			s.logger.ErrorContext(ctx, "Xendit bank upsert failed", "channel_code", channel.Code, "error", err)
		}
	}

	ilumaResult := s.iluma.BankList(ctx)
	envelope := ilumaResult.LegacyBankListEnvelope()
	if err := s.store.InsertIlumaCallLog(ctx, storage.IlumaCallLog{
		URL: "https://api.iluma.ai/bank/available_bank_codes", Method: "get",
		Function: "yggdrasilService.refund", Response: envelope,
	}); err != nil {
		return err
	}
	if ilumaResult.Status != http.StatusOK || ilumaResult.Error != nil {
		if ilumaResult.Error != nil {
			return errors.New(ilumaResult.Error.Message)
		}
		return errors.New("Iluma bank synchronization failed")
	}
	var banks []map[string]any
	if err := json.Unmarshal(ilumaResult.Data, &banks); err != nil {
		return fmt.Errorf("decode Iluma bank list: %w", err)
	}
	for _, item := range banks {
		name, _ := item["name"].(string)
		code, _ := item["code"].(string)
		data, _ := json.Marshal(item)
		updated, err := s.store.UpdateIlumaRefundBankByName(ctx, strings.TrimSpace(name), strings.TrimSpace(code), data)
		if err != nil {
			return err
		}
		if !updated {
			s.logger.WarnContext(ctx, "Iluma bank not found in refund_banks", "bank_name", name)
		}
	}
	return nil
}

func record(value storage.RefundBank) Record {
	result := Record{
		ID: value.ID, BankName: value.BankName, XenditCode: value.XenditCode,
		XenditData: nullJSON(value.XenditData), IlumaCode: value.IlumaCode,
		IlumaData: nullJSON(value.IlumaData), BankStatus: string(value.Status),
		CreatedAt: jsTime(value.CreatedAt), UpdatedAt: jsTime(value.UpdatedAt),
	}
	if value.DeletedAt != nil {
		formatted := jsTime(*value.DeletedAt)
		result.DeletedAt = &formatted
	}
	return result
}

func records(values []storage.RefundBank) []Record {
	result := make([]Record, 0, len(values))
	for _, value := range values {
		result = append(result, record(value))
	}
	return result
}

func nullJSON(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage("null")
	}
	return value
}

func jsTime(value time.Time) string { return value.UTC().Format("2006-01-02T15:04:05.000Z") }

type XenditClient struct {
	baseURL string
	http    *http.Client
}

func NewXenditClient(baseURL string) *XenditClient {
	// The Node SDK does not set a per-call timeout for this operation.
	return &XenditClient{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{}}
}

func (c *XenditClient) PayoutChannels(ctx context.Context, secret string) ([]Channel, error) {
	endpoint, err := url.Parse(c.baseURL + "/payouts_channels")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("currency", "IDR")
	query.Add("channel_category", "BANK")
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(secret+":")))
	request.Header.Set("Accept", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("list Xendit payout channels: %w", err)
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("list Xendit payout channels: HTTP %d", response.StatusCode)
	}
	var rawChannels []map[string]any
	if err := json.Unmarshal(contents, &rawChannels); err != nil {
		return nil, fmt.Errorf("decode Xendit payout channels: %w", err)
	}
	result := make([]Channel, 0, len(rawChannels))
	for _, raw := range rawChannels {
		converted := camelizeMap(raw)
		code, _ := converted["channelCode"].(string)
		name, _ := converted["channelName"].(string)
		encoded, _ := json.Marshal(converted)
		if strings.TrimSpace(code) == "" || strings.TrimSpace(name) == "" {
			continue
		}
		result = append(result, Channel{Code: code, Name: name, Data: encoded})
	}
	return result, nil
}

func camelizeMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[camelKey(key)] = camelizeValue(value)
	}
	return result
}

func camelizeValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return camelizeMap(typed)
	case []any:
		result := make([]any, len(typed))
		for index := range typed {
			result[index] = camelizeValue(typed[index])
		}
		return result
	default:
		return value
	}
}

func camelKey(value string) string {
	var result []rune
	upper := false
	for _, character := range value {
		if character == '_' || character == '-' {
			upper = true
			continue
		}
		if upper {
			character = unicode.ToUpper(character)
			upper = false
		}
		result = append(result, character)
	}
	return string(result)
}
