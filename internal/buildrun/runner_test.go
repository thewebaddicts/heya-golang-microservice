package buildrun

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"heya-golang-microservice/internal/config"
)

func TestShellBuildCommandUsesNPMBuild(t *testing.T) {
	got := shellBuildCommand("npm")
	want := "'npm' run build"
	if got != want {
		t.Fatalf("shellBuildCommand() = %q, want %q", got, want)
	}
}

func TestRunnerStreamsOutputAndCompletes(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`console.log("building"); console.error("warning");`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	runner := NewRunner(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		BuildRootDir:      t.TempDir(),
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	})

	var outputs []Output
	complete, err := runner.Run(context.Background(), Request{}, nil, func(output Output) error {
		outputs = append(outputs, output)
		return nil
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if complete.Status != "success" || complete.ExitCode != 0 {
		t.Fatalf("complete = %#v", complete)
	}
	if complete.Mode != ModeSafe {
		t.Fatalf("complete.Mode = %q, want %q", complete.Mode, ModeSafe)
	}
	if complete.BuildProjectPath == projectDir {
		t.Fatalf("BuildProjectPath = source dir %q, want isolated workspace", projectDir)
	}
	if len(outputs) == 0 {
		t.Fatal("outputs is empty")
	}
}

func TestRunnerSafeModeExcludesLiveOutputFolders(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"mkdir -p .output && node -e \"console.log('ok')\""}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(projectDir, ".output"), 0o755); err != nil {
		t.Fatalf("create live output dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, ".output", "live.txt"), []byte("live"), 0o644); err != nil {
		t.Fatalf("write live output: %v", err)
	}

	runner := NewRunner(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		BuildRootDir:      t.TempDir(),
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	})

	complete, err := runner.Run(context.Background(), Request{}, nil, nil)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(complete.BuildProjectPath, ".output", "live.txt")); !os.IsNotExist(err) {
		t.Fatalf("live output was copied into safe workspace, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(projectDir, ".output", "live.txt")); err != nil {
		t.Fatalf("live output was mutated: %v", err)
	}
}

func TestRunnerReportsFailureExitCode(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`process.exit(7);`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	runner := NewRunner(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		BuildRootDir:      t.TempDir(),
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	})

	complete, err := runner.Run(context.Background(), Request{}, nil, nil)
	if err == nil {
		t.Fatal("Run() error = nil, want error")
	}
	if complete.Status != "failed" || complete.ExitCode != 7 {
		t.Fatalf("complete = %#v", complete)
	}
}

func TestRunnerCanBeCanceled(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`setTimeout(() => {}, 10000);`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	runner := NewRunner(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		BuildRootDir:      t.TempDir(),
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	complete, err := runner.Run(ctx, Request{}, nil, nil)
	if err == nil {
		t.Fatal("Run() error = nil, want error")
	}
	if complete.Status != "canceled" {
		t.Fatalf("complete.Status = %q, want %q", complete.Status, "canceled")
	}
}

func TestRunnerRejectsConcurrentBuildForSameProject(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`setTimeout(() => {}, 1000);`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	runner := NewRunner(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		BuildRootDir:      t.TempDir(),
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = runner.Run(ctx, Request{Mode: ModeLive}, func(Started) error {
			close(started)
			return nil
		}, nil)
	}()
	<-started

	_, err := runner.Run(context.Background(), Request{Mode: ModeLive}, nil, nil)
	if err == nil {
		t.Fatal("second Run() error = nil, want running build error")
	}
	cancel()
	<-done
}
