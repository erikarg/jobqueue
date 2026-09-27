// Command jobqueue runs the HTTP API and the worker pool in one process.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/erikarg/jobqueue/internal/api"
	"github.com/erikarg/jobqueue/internal/metrics"
	"github.com/erikarg/jobqueue/internal/store"
	"github.com/erikarg/jobqueue/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// ctx is cancelled on SIGINT/SIGTERM and starts the graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	st := store.New(db)
	if err := st.Migrate(ctx); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	pool, err := worker.New(st, handlers, cfg.Worker, log, m)
	if err != nil {
		return err
	}
	poolDone := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(poolDone)
	}()

	srv := api.Server{Store: st, Handlers: handlers, MaxAttempts: cfg.MaxAttempts, Metrics: m, Gatherer: reg, Log: log}
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr, "workers", cfg.Worker.Workers)
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err = <-serveErr:
		log.Error("http server failed", "error", err)
	}
	// Cancels the pool (if the HTTP server failed) and restores default signal
	// handling, so a second Ctrl-C kills a shutdown stuck on a misbehaving handler.
	stop()

	// Fail readiness first and keep serving for DrainDelay so load balancers
	// notice and stop routing here; then stop accepting HTTP traffic. The pool
	// is already draining (it reacts to ctx).
	srv.SetShuttingDown()
	time.Sleep(cfg.DrainDelay)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}
	<-poolDone
	log.Info("shutdown complete")
	return err // nil unless the HTTP server failed
}
