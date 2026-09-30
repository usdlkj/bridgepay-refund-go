package httpapi

import (
	"context"
	"net/http"
)

func withRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

func RequestID(r *http.Request) string {
	value, _ := r.Context().Value(requestIDKey).(string)
	return value
}
