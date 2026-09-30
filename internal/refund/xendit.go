package refund

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type PayoutRequest struct {
	ReferenceID       string                  `json:"reference_id"`
	ChannelCode       string                  `json:"channel_code"`
	ChannelProperties PayoutChannelProperties `json:"channel_properties"`
	Amount            int64                   `json:"amount"`
	Description       string                  `json:"description"`
	Currency          string                  `json:"currency"`
	IdempotencyKey    string                  `json:"idempotencyKey"`
}

type PayoutChannelProperties struct {
	AccountNumber     string `json:"account_number"`
	AccountHolderName string `json:"account_holder_name"`
}

type ProviderResult struct {
	Status int
	Data   json.RawMessage
	Err    error
}

type PayoutProvider interface {
	Create(context.Context, string, string, PayoutRequest) ProviderResult
	Status(context.Context, string, string) ProviderResult
}

type XenditClient struct {
	baseURL string
	http    *http.Client
}

func NewXenditClient(baseURL string) *XenditClient {
	// Axios has no configured timeout in the frozen Node payout flow.
	return &XenditClient{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{}}
}

func (c *XenditClient) Create(ctx context.Context, secret, idempotencyKey string, payload PayoutRequest) ProviderResult {
	body, err := json.Marshal(payload)
	if err != nil {
		return ProviderResult{Status: 500, Err: err}
	}
	return c.do(ctx, http.MethodPost, c.baseURL+"/v2/payouts", secret, idempotencyKey, body)
}

func (c *XenditClient) Status(ctx context.Context, secret, payoutID string) ProviderResult {
	return c.do(ctx, http.MethodGet, c.baseURL+"/v2/payouts/"+url.PathEscape(payoutID), secret, "", nil)
}

func (c *XenditClient) Balance(ctx context.Context, secret string) ProviderResult {
	return c.do(ctx, http.MethodGet, c.baseURL+"/balance", secret, "", nil)
}

func (c *XenditClient) do(ctx context.Context, method, endpoint, secret, idempotency string, body []byte) ProviderResult {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return ProviderResult{Status: 500, Err: err}
	}
	request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(secret+":")))
	request.Header.Set("Content-Type", "application/json")
	if idempotency != "" {
		request.Header.Set("Idempotency-key", idempotency)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return ProviderResult{Status: 500, Err: fmt.Errorf("Xendit payout request: %w", err)}
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return ProviderResult{Status: 500, Err: err}
	}
	return ProviderResult{Status: response.StatusCode, Data: contents}
}
