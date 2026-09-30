package refund

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type TicketingResult struct {
	Status                int
	Body                  any
	Raw                   json.RawMessage
	Err                   error
	StartedAt, FinishedAt time.Time
}

func (r TicketingResult) Accepted() bool {
	if r.Status != http.StatusOK && r.Status != http.StatusCreated {
		return false
	}
	object, ok := r.Body.(map[string]any)
	if !ok {
		return false
	}
	code, ok := object["retCode"].(json.Number)
	if ok {
		return code.String() == "0"
	}
	value, ok := object["retCode"].(float64)
	return ok && value == 0
}

func (r TicketingResult) safeJSON() json.RawMessage {
	if len(r.Raw) > 0 && json.Valid(r.Raw) {
		return r.Raw
	}
	result, _ := json.Marshal(map[string]any{"status": r.Status, "error": errorText(r.Err)})
	return result
}

type TicketingClient struct{ http *http.Client }

func NewTicketingClient() *TicketingClient {
	return &TicketingClient{http: &http.Client{Timeout: 30 * time.Second}}
}

func (c *TicketingClient) Notify(ctx context.Context, endpoint string, payload any) TicketingResult {
	result := TicketingResult{StartedAt: time.Now()}
	body, err := json.Marshal(payload)
	if err != nil {
		result.Err, result.FinishedAt = err, time.Now()
		return result
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		result.Err, result.FinishedAt = err, time.Now()
		return result
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		result.Err, result.FinishedAt = fmt.Errorf("Ticketing notification: %w", err), time.Now()
		return result
	}
	defer response.Body.Close()
	result.Status = response.StatusCode
	result.Raw, err = io.ReadAll(io.LimitReader(response.Body, 4<<20))
	result.FinishedAt = time.Now()
	if err != nil {
		result.Err = err
		return result
	}
	decoder := json.NewDecoder(bytes.NewReader(result.Raw))
	decoder.UseNumber()
	if err := decoder.Decode(&result.Body); err != nil {
		result.Body = map[string]any{"body": string(result.Raw)}
	}
	return result
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
