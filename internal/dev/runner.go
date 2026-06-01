package dev

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"heya-golang-microservice/internal/config"
)

type RunRequest struct {
	ProjectPath   string
	Port          int
	DevServerHost string
}

type RunResult struct {
	ProjectPath  string    `json:"projectPath"`
	Port         int       `json:"port"`
	DevServerURL string    `json:"devServerURL"`
	PID          string    `json:"pid"`
	LogFile      string    `json:"logFile"`
	Command      string    `json:"command"`
	Target       string    `json:"target"`
	StartedAt    time.Time `json:"startedAt"`
}

type Runner interface {
	Run(context.Context, RunRequest) (RunResult, error)
	Stop(context.Context, RunResult) error
}

type LocalRunner struct {
	cfg    config.Config
	logger *slog.Logger
}

func NewLocalRunner(cfg config.Config, logger *slog.Logger) *LocalRunner {
	return &LocalRunner{cfg: cfg, logger: logger}
}

func (r *LocalRunner) Run(ctx context.Context, req RunRequest) (RunResult, error) {
	port := req.Port
	if port == 0 {
		port = r.cfg.DefaultDevPort
	}
	if port < 1 || port > 65535 {
		return RunResult{}, config.ValidationError("port must be between 1 and 65535")
	}

	projectDir, err := r.cfg.ResolveProjectDir(req.ProjectPath)
	if err != nil {
		return RunResult{}, err
	}
	if err := validateProjectDir(projectDir); err != nil {
		return RunResult{}, err
	}

	startedAt := time.Now().UTC()
	logFile := localLogFile(r.cfg.LogDir, projectDir, port, startedAt)
	if err := os.MkdirAll(filepath.Dir(logFile), 0o755); err != nil {
		return RunResult{}, fmt.Errorf("create log directory: %w", err)
	}

	logWriter, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return RunResult{}, fmt.Errorf("open dev server log file: %w", err)
	}
	defer logWriter.Close()

	command := shellDevCommand(r.cfg.NPMBin, port)
	cmd := exec.Command(r.cfg.CommandShell, shellArgs(r.cfg.CommandShell, command)...)
	cmd.Dir = projectDir
	cmd.Env = shellEnvironment(os.Environ())
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return RunResult{}, fmt.Errorf("start local dev command: %w", err)
	}

	exited := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		exited <- err
		if err != nil {
			r.logger.Info("local SolidJS dev server process exited", "pid", cmd.Process.Pid, "error", err)
			return
		}
		r.logger.Info("local SolidJS dev server process exited", "pid", cmd.Process.Pid)
	}()

	result := RunResult{
		ProjectPath:  projectDir,
		Port:         port,
		DevServerURL: r.devServerURL(port, req.DevServerHost),
		PID:          strconv.Itoa(cmd.Process.Pid),
		LogFile:      logFile,
		Command:      command,
		Target:       "local",
		StartedAt:    startedAt,
	}

	r.logger.Info("started SolidJS dev server",
		"projectPath", result.ProjectPath,
		"port", result.Port,
		"pid", result.PID,
		"target", result.Target,
		"logFile", result.LogFile,
	)

	if err := r.waitUntilReady(ctx, result.DevServerURL, exited); err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), r.processStopTimeout())
		defer cancel()
		_ = terminateProcessGroup(stopCtx, cmd.Process.Pid)
		return RunResult{}, err
	}

	r.logger.Info("SolidJS dev server is ready",
		"projectPath", result.ProjectPath,
		"port", result.Port,
		"url", result.DevServerURL,
		"pid", result.PID,
	)

	return result, nil
}

func (r *LocalRunner) devServerURL(port int, hostOverride string) string {
	scheme := strings.TrimSpace(r.cfg.DevServerScheme)
	if scheme == "" {
		scheme = "http"
	}
	host := strings.TrimSpace(hostOverride)
	if host == "" {
		host = strings.TrimSpace(r.cfg.DevServerHost)
	}
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("%s://%s:%d", scheme, host, port)
}

func (r *LocalRunner) waitUntilReady(ctx context.Context, url string, exited <-chan error) error {
	readyCtx, cancel := context.WithTimeout(ctx, r.devReadyTimeout())
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		ready, err := probeHTTPReady(readyCtx, url)
		if ready {
			return nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case err := <-exited:
			if err != nil {
				return fmt.Errorf("dev server process exited before it was ready: %w", err)
			}
			return errors.New("dev server process exited before it was ready")
		case <-readyCtx.Done():
			if lastErr != nil {
				return fmt.Errorf("dev server was not ready at %s before timeout: %w", url, lastErr)
			}
			return fmt.Errorf("dev server was not ready at %s before timeout: %w", url, readyCtx.Err())
		case <-ticker.C:
		}
	}
}

func probeHTTPReady(ctx context.Context, url string) (bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode >= 200 && resp.StatusCode < 500, nil
}

func (r *LocalRunner) devReadyTimeout() time.Duration {
	if r.cfg.DevReadyTimeout > 0 {
		return r.cfg.DevReadyTimeout
	}
	return 60 * time.Second
}

func (r *LocalRunner) processStopTimeout() time.Duration {
	if r.cfg.ProcessStopTimeout > 0 {
		return r.cfg.ProcessStopTimeout
	}
	return 15 * time.Second
}

func validateProjectDir(projectDir string) error {
	info, err := os.Stat(projectDir)
	if err != nil {
		return fmt.Errorf("project directory is not accessible: %w", err)
	}
	if !info.IsDir() {
		return config.ValidationError("projectPath must be a directory")
	}
	if _, err := os.Stat(filepath.Join(projectDir, "package.json")); err != nil {
		return fmt.Errorf("project directory must contain package.json: %w", err)
	}
	return nil
}

func shellDevCommand(npmBin string, port int) string {
	npmBin = strings.TrimSpace(npmBin)
	if npmBin == "" {
		npmBin = "npm"
	}
	return shellQuote(npmBin) + " run dev -- --port " + strconv.Itoa(port)
}

func shellArgs(shellPath, command string) []string {
	switch filepath.Base(shellPath) {
	case "zsh", "bash":
		return []string{"-lc", command}
	default:
		return []string{"-c", command}
	}
}

func shellEnvironment(env []string) []string {
	return prependPath(env, "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
}

func prependPath(env []string, prefix string) []string {
	if prefix == "" {
		return env
	}
	for i, entry := range env {
		if strings.HasPrefix(entry, "PATH=") {
			current := strings.TrimPrefix(entry, "PATH=")
			if current == "" {
				env[i] = "PATH=" + prefix
				return env
			}
			env[i] = "PATH=" + prefix + string(os.PathListSeparator) + current
			return env
		}
	}
	return append(env, "PATH="+prefix)
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func (r *LocalRunner) Stop(ctx context.Context, run RunResult) error {
	pid, err := parseProcessID(run.PID)
	if err != nil {
		return err
	}

	return terminateProcessGroup(ctx, pid)
}

func localLogFile(logDir, projectDir string, port int, startedAt time.Time) string {
	projectName := filepath.Base(projectDir)
	projectName = regexp.MustCompile(`[^a-zA-Z0-9._-]+`).ReplaceAllString(projectName, "-")
	if projectName == "" || projectName == "." || projectName == "/" {
		projectName = "solidjs-project"
	}

	fileName := fmt.Sprintf("%s-%d-%s.log", projectName, port, startedAt.Format("20060102T150405Z"))
	return filepath.Join(logDir, fileName)
}

func parseProcessID(pid string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(pid))
	if err != nil {
		return 0, fmt.Errorf("invalid pid %q: %w", pid, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("invalid pid %q", pid)
	}
	return value, nil
}

func terminateProcessGroup(ctx context.Context, pid int) error {
	if !processGroupExists(pid) {
		return nil
	}

	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("send SIGTERM to process group %d: %w", pid, err)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if !processGroupExists(pid) {
			return nil
		}

		select {
		case <-ctx.Done():
			if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				return fmt.Errorf("send SIGKILL to process group %d: %w", pid, err)
			}
			return fmt.Errorf("timed out stopping process group %d: %w", pid, ctx.Err())
		case <-ticker.C:
		}
	}
}

func processGroupExists(pid int) bool {
	err := syscall.Kill(-pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
