package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/erikarg/jobqueue/internal/worker"
)

type config struct {
	HTTPAddr    string
	DatabaseURL string
	MaxAttempts int
	DrainDelay  time.Duration
	Worker      worker.Config
}

// loadConfig reads configuration from environment variables, falling back to
// defaults suitable for local development.
func loadConfig() (config, error) {
	var errs []error
	env := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	envInt := func(key string, def int) int {
		n, err := strconv.Atoi(env(key, strconv.Itoa(def)))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
		}
		return n
	}
	envDuration := func(key string, def time.Duration) time.Duration {
		d, err := time.ParseDuration(env(key, def.String()))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
		}
		return d
	}

	cfg := config{
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		DatabaseURL: env("DATABASE_URL", "postgres://jobqueue:jobqueue@localhost:5433/jobqueue?sslmode=disable"),
		MaxAttempts: envInt("MAX_ATTEMPTS", 5),
		DrainDelay:  envDuration("DRAIN_DELAY", 0),
		Worker: worker.Config{
			Workers:          envInt("WORKERS", 4),
			PollInterval:     envDuration("POLL_INTERVAL", time.Second),
			JobTimeout:       envDuration("JOB_TIMEOUT", 30*time.Second),
			LeaseDuration:    envDuration("LEASE_DURATION", time.Minute),
			RecoveryInterval: envDuration("RECOVERY_INTERVAL", 30*time.Second),
			ShutdownTimeout:  envDuration("SHUTDOWN_TIMEOUT", 20*time.Second),
			BackoffBase:      envDuration("BACKOFF_BASE", time.Second),
			BackoffMax:       envDuration("BACKOFF_MAX", 5*time.Minute),
		},
	}
	if cfg.MaxAttempts < 1 {
		errs = append(errs, errors.New("MAX_ATTEMPTS must be at least 1"))
	}
	return cfg, errors.Join(errs...)
}
