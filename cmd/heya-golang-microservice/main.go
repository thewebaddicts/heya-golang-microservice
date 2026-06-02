package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"heya-golang-microservice/internal/config"
	"heya-golang-microservice/internal/dev"
	"heya-golang-microservice/internal/httpapi"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logger.Info("loaded config",
		"projectBaseDir", cfg.ProjectBaseDir,
		"defaultProjectDir", cfg.DefaultProjectDir,
		"defaultDevPort", cfg.DefaultDevPort,
		"commandShell", cfg.CommandShell,
		"devReadyHost", cfg.DevReadyHost,
		"webSocketAllowedOrigins", cfg.WebSocketAllowedOrigins,
		"devReadyTimeout", cfg.DevReadyTimeout,
		"devIdleTimeout", cfg.DevIdleTimeout,
		"accountInfoURL", cfg.AccountInfoURL,
		"accountInfoTokenSet", cfg.AccountInfoToken != "",
		"accountInfoTimeout", cfg.AccountInfoTimeout,
	)

	runner := dev.NewLocalRunner(cfg, logger)
	api := httpapi.NewServer(cfg, runner, logger)

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting HTTP server", "addr", cfg.HTTPAddr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to gracefully shutdown HTTP server", "error", err)
			os.Exit(1)
		}
		logger.Info("HTTP server stopped")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}
}
