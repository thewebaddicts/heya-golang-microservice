package dev

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"heya-golang-microservice/internal/config"
)

type fakeRunner struct {
	runCount  int
	stopCount int
}

func (r *fakeRunner) Run(context.Context, RunRequest) (RunResult, error) {
	r.runCount++
	return RunResult{
		ProjectPath:  "/tmp/solid-app",
		Port:         3002,
		DevServerURL: "http://localhost:3002",
		PID:          "12345",
		StartedAt:    time.Now().UTC(),
	}, nil
}

func (r *fakeRunner) Stop(context.Context, RunResult) error {
	r.stopCount++
	return nil
}

func TestManagerReusesRunningServerUntilLastRelease(t *testing.T) {
	runner := &fakeRunner{}
	manager := NewManager(config.Config{
		ProjectBaseDir:    "/tmp",
		DefaultProjectDir: "/tmp/solid-app",
		DefaultDevPort:    3002,
		DevIdleTimeout:    20 * time.Millisecond,
	}, runner, slog.Default())

	first, err := manager.Acquire(context.Background(), RunRequest{})
	if err != nil {
		t.Fatalf("first Acquire() error = %v", err)
	}
	second, err := manager.Acquire(context.Background(), RunRequest{})
	if err != nil {
		t.Fatalf("second Acquire() error = %v", err)
	}

	if runner.runCount != 1 {
		t.Fatalf("runCount = %d, want 1", runner.runCount)
	}
	if first.Count != 1 {
		t.Fatalf("first.Count = %d, want 1", first.Count)
	}
	if second.Count != 2 {
		t.Fatalf("second.Count = %d, want 2", second.Count)
	}

	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if runner.stopCount != 0 {
		t.Fatalf("stopCount after first release = %d, want 0", runner.stopCount)
	}

	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	if runner.stopCount != 0 {
		t.Fatalf("stopCount immediately after second release = %d, want 0", runner.stopCount)
	}
	waitForManagerStopCount(t, runner, 1)
}

func TestManagerCancelsIdleStopWhenConnectionReturns(t *testing.T) {
	runner := &fakeRunner{}
	manager := NewManager(config.Config{
		ProjectBaseDir:    "/tmp",
		DefaultProjectDir: "/tmp/solid-app",
		DefaultDevPort:    3002,
		DevIdleTimeout:    50 * time.Millisecond,
	}, runner, slog.Default())

	first, err := manager.Acquire(context.Background(), RunRequest{})
	if err != nil {
		t.Fatalf("first Acquire() error = %v", err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}

	second, err := manager.Acquire(context.Background(), RunRequest{})
	if err != nil {
		t.Fatalf("second Acquire() error = %v", err)
	}
	if runner.runCount != 1 {
		t.Fatalf("runCount = %d, want 1", runner.runCount)
	}
	if second.Count != 1 {
		t.Fatalf("second.Count = %d, want 1", second.Count)
	}

	time.Sleep(75 * time.Millisecond)
	if runner.stopCount != 0 {
		t.Fatalf("stopCount after canceled idle timeout = %d, want 0", runner.stopCount)
	}

	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	waitForManagerStopCount(t, runner, 1)
}

func waitForManagerStopCount(t *testing.T, runner *fakeRunner, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if runner.stopCount == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if runner.stopCount != want {
		t.Fatalf("stopCount = %d, want %d", runner.stopCount, want)
	}
}
