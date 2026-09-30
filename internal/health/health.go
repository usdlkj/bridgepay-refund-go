package health

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type Status string

const (
	StatusUp      Status = "up"
	StatusDown    Status = "down"
	StatusSkipped Status = "skipped"
)

type Result struct {
	Name      string `json:"name"`
	Status    Status `json:"status"`
	LatencyMS int64  `json:"latencyMs"`
	err       error
}

type Report struct {
	Status string   `json:"status"`
	Checks []Result `json:"checks"`
}

type Check struct {
	Name     string
	Disabled bool
	Run      func(context.Context) error
}

type Service struct {
	timeout time.Duration
	checks  []Check
}

func New(timeout time.Duration, checks ...Check) *Service {
	return &Service{timeout: timeout, checks: checks}
}

func (s *Service) Readiness(ctx context.Context) Report {
	type indexed struct {
		index  int
		result Result
	}
	results := make(chan indexed, len(s.checks))
	for index, check := range s.checks {
		go func(index int, check Check) {
			started := time.Now()
			if check.Disabled {
				results <- indexed{index: index, result: Result{Name: check.Name, Status: StatusSkipped}}
				return
			}
			checkCtx, cancel := context.WithTimeout(ctx, s.timeout)
			defer cancel()
			err := check.Run(checkCtx)
			status := StatusUp
			if err != nil {
				status = StatusDown
			}
			results <- indexed{index: index, result: Result{
				Name: check.Name, Status: status, LatencyMS: time.Since(started).Milliseconds(), err: err,
			}}
		}(index, check)
	}

	report := Report{Status: "ok", Checks: make([]Result, len(s.checks))}
	for range s.checks {
		item := <-results
		report.Checks[item.index] = item.result
		if item.result.Status == StatusDown {
			report.Status = "error"
		}
	}
	return report
}

func (r Report) FailedNames() []string {
	names := make([]string, 0)
	for _, check := range r.Checks {
		if check.Status == StatusDown {
			names = append(names, check.Name)
		}
	}
	sort.Strings(names)
	return names
}

func PostgreSQL(pool *pgxpool.Pool) Check {
	return Check{Name: "postgresql", Run: pool.Ping}
}

func Redis(client *redis.Client) Check {
	return Check{Name: "redis", Run: func(ctx context.Context) error { return client.Ping(ctx).Err() }}
}

func RabbitMQ(rawURL string) Check {
	return Check{Name: "rabbitmq", Run: func(ctx context.Context) error {
		conn, err := dialRabbitMQ(ctx, rawURL)
		if err != nil {
			return err
		}
		return conn.Close()
	}}
}

func RabbitMQQueue(rawURL, name, queue string) Check {
	return Check{Name: name, Run: func(ctx context.Context) error {
		conn, err := dialRabbitMQ(ctx, rawURL)
		if err != nil {
			return err
		}
		defer conn.Close()
		channel, err := conn.Channel()
		if err != nil {
			return err
		}
		defer channel.Close()
		_, err = channel.QueueInspect(queue)
		return err
	}}
}

func TCP(name, address string) Check {
	return Check{Name: name, Run: func(ctx context.Context) error {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err != nil {
			return err
		}
		return conn.Close()
	}}
}

func HTTPReachability(name, rawURL string, enabled bool) Check {
	return Check{Name: name, Disabled: !enabled, Run: func(ctx context.Context) error {
		u, err := url.Parse(rawURL)
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
		if err != nil {
			return err
		}
		transport := &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			DialContext:     (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		}
		client := &http.Client{Transport: transport}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= http.StatusInternalServerError {
			return &statusError{code: resp.StatusCode}
		}
		return nil
	}}
}

type statusError struct{ code int }

func (e *statusError) Error() string { return http.StatusText(e.code) }

func dialRabbitMQ(ctx context.Context, rawURL string) (*amqp091.Connection, error) {
	dialer := &net.Dialer{}
	config := amqp091.Config{
		Dial: func(network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	}
	return amqp091.DialConfig(rawURL, config)
}
