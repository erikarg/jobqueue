package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/erikarg/jobqueue/internal/job"
)

// Example job handlers. Real deployments register their own; these exist to
// exercise timeouts, retries and error classification end to end.
var handlers = map[string]job.Handler{
	"sleep":   sleep,
	"webhook": webhook,
}

// sleep waits for the given duration, honoring cancellation.
// Payload: {"duration": "2s"}
func sleep(ctx context.Context, payload json.RawMessage) error {
	var p struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return job.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	d, err := time.ParseDuration(p.Duration)
	if err != nil {
		return job.Permanent(fmt.Errorf("invalid duration: %w", err))
	}

	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// webhook POSTs a JSON body to a URL. Network errors, 429 and 5xx responses
// are retryable; other 4xx responses are permanent (retrying won't help).
// Payload: {"url": "https://example.com/hook", "body": {...}}
func webhook(ctx context.Context, payload json.RawMessage) error {
	var p struct {
		URL  string          `json:"url"`
		Body json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return job.Permanent(fmt.Errorf("decode payload: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(p.Body))
	if err != nil {
		return job.Permanent(fmt.Errorf("build request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")

	// No client timeout: the job context carries the deadline.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))

	switch {
	case resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("webhook returned %s", resp.Status)
	default:
		return job.Permanent(fmt.Errorf("webhook returned %s", resp.Status))
	}
}
