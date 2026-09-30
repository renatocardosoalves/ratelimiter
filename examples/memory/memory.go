// Command memory runs a sample REST API protected by the rate limiter using
// the in-memory storage.
//
//	go run examples/memory/memory.go
//	go run examples/memory/memory.go -limit 10 -window 1s -block 5s -log-format json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/renatocardosoalves/ratelimiter/middleware"
	"github.com/renatocardosoalves/ratelimiter/ratelimiter"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	limit := flag.Int("limit", ratelimiter.DefaultLimit, "maximum requests per window")
	window := flag.Duration("window", ratelimiter.DefaultWindow, "counting window (e.g. 30s, 10m, 1h)")
	block := flag.Duration("block", ratelimiter.DefaultBlockDuration, "block duration after exceeding the limit")
	jsonBody := flag.Bool("json", true, "send the JSON body on 429 responses")
	logFormat := flag.String("log-format", "text", "log format: text or json")
	debug := flag.Bool("debug", false, "log every request (debug level)")
	flag.Parse()

	logger := newLogger(*logFormat, *debug)
	slog.SetDefault(logger)

	limiter, err := ratelimiter.New(
		ratelimiter.WithLimit(*limit),
		ratelimiter.WithWindow(*window),
		ratelimiter.WithBlockDuration(*block),
		ratelimiter.WithLogger(logger),
	)
	if err != nil {
		logger.Error("invalid rate limiter configuration", slog.Any("error", err))
		os.Exit(1)
	}
	defer limiter.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", hello)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           middleware.RateLimit(limiter, middleware.WithJSONResponse(*jsonBody))(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("server started",
		slog.String("addr", *addr),
		slog.Int("limit", *limit),
		slog.Duration("window", *window),
		slog.Duration("block", *block),
	)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server failed", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("server stopped")
}

func hello(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"message":   "Hello, World!",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func newLogger(format string, debug bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if debug {
		opts.Level = slog.LevelDebug
	}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}
