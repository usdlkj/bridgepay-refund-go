package iluma

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultBankListURL = "https://api.iluma.ai/bank/available_bank_codes"

type ProviderError struct {
	Code       string         `json:"code"`
	Message    string         `json:"message"`
	Detail     map[string]any `json:"detail"`
	HTTPStatus int            `json:"httpStatus"`
}

type HTTPResult struct {
	Status int
	Data   json.RawMessage
	Error  *ProviderError
}

type Client struct {
	baseURL     string
	bankListURL string
	token       string
	http        *http.Client
}

type ClientOption func(*Client)

func WithBankListURL(value string) ClientOption {
	return func(client *Client) { client.bankListURL = value }
}

func NewClient(baseURL, token string, timeout time.Duration, options ...ClientOption) *Client {
	client := &Client{
		baseURL:     strings.TrimRight(baseURL, "/") + "/v1.2/identity",
		bankListURL: defaultBankListURL,
		token:       token,
		http:        &http.Client{Timeout: timeout},
	}
	for _, option := range options {
		option(client)
	}
	return client
}

func (c *Client) Validate(ctx context.Context, bankCode, accountNumber string) HTTPResult {
	body, err := json.Marshal(map[string]string{"bank_code": bankCode, "bank_account_number": accountNumber})
	if err != nil {
		return networkFailure(err)
	}
	return c.do(ctx, http.MethodPost, c.baseURL+"/bank_account_validation_details", body)
}

func (c *Client) Result(ctx context.Context, requestID string) HTTPResult {
	return c.do(ctx, http.MethodGet, c.baseURL+"/bank_account_validation_details/"+url.PathEscape(requestID), nil)
}

// BankList preserves the legacy adapter: its endpoint is fixed and every
// failure is returned as {status:500,msg}, regardless of provider status.
func (c *Client) BankList(ctx context.Context) HTTPResult {
	result := c.do(ctx, http.MethodGet, c.bankListURL, nil)
	if result.Error != nil {
		result.Status = http.StatusInternalServerError
	}
	return result
}

func (c *Client) do(ctx context.Context, method, endpoint string, body []byte) HTTPResult {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return networkFailure(err)
	}
	request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.token+":")))
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return networkFailure(err)
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return networkFailure(err)
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return HTTPResult{Status: response.StatusCode, Data: contents}
	}
	return HTTPResult{Status: response.StatusCode, Data: contents, Error: normalizeHTTPError(response.StatusCode, response.Status, contents)}
}

func networkFailure(err error) HTTPResult {
	code := "ILUMA_ERROR"
	message := err.Error()
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(message), "timeout") {
		code = "ILUMA_TIMEOUT"
	} else {
		var dnsError *url.Error
		if errors.As(err, &dnsError) {
			code = "ILUMA_NETWORK_ERROR"
		}
	}
	providerError := &ProviderError{Code: code, Message: message, HTTPStatus: http.StatusInternalServerError, Detail: map[string]any{"code": code}}
	data, _ := json.Marshal(providerError)
	return HTTPResult{Status: providerError.HTTPStatus, Data: data, Error: providerError}
}

func normalizeHTTPError(status int, statusText string, contents []byte) *ProviderError {
	code := "ILUMA_ERROR"
	switch status {
	case http.StatusUnauthorized:
		code = "ILUMA_AUTH_FAILED"
	case http.StatusRequestTimeout:
		code = "ILUMA_TIMEOUT"
	case http.StatusTooManyRequests:
		code = "ILUMA_RATE_LIMIT"
	case http.StatusInternalServerError:
		code = "ILUMA_SERVER_ERROR"
	}
	message := "Iluma error"
	var response map[string]any
	if json.Unmarshal(contents, &response) == nil {
		if value, ok := response["message"].(string); ok && value != "" {
			message = value
		}
	} else if statusText != "" {
		message = statusText
	}
	return &ProviderError{Code: code, Message: message, HTTPStatus: status, Detail: map[string]any{"responseStatus": status, "responseData": response}}
}

func (r HTTPResult) LegacyBankListEnvelope() json.RawMessage {
	if r.Error != nil {
		value, _ := json.Marshal(map[string]any{"status": 500, "msg": r.Error.Message})
		return value
	}
	var data any
	if json.Unmarshal(r.Data, &data) != nil {
		data = nil
	}
	value, _ := json.Marshal(map[string]any{"status": 200, "data": data})
	return value
}

func decodeObject(raw json.RawMessage) (map[string]any, error) {
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("decode Iluma response: %w", err)
	}
	return value, nil
}
