package buildrun

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"heya-golang-microservice/internal/config"
)

const (
	ModeSafe = "safe"
	ModeLive = "live"
)

type Request struct {
	ProjectPath string
	Mode        string
}

type Started struct {
	Mode              string    `json:"mode"`
	SourceProjectPath string    `json:"sourceProjectPath"`
	BuildProjectPath  string    `json:"buildProjectPath"`
	Command           string    `json:"command"`
	PID               string    `json:"pid"`
	StartedAt         time.Time `json:"startedAt"`
}

type Output struct {
	Stream string `json:"stream"`
	Data   string `json:"data"`
}

type Complete struct {
	Mode              string    `json:"mode"`
	Status            string    `json:"status"`
	ExitCode          int       `json:"exitCode"`
	SourceProjectPath string    `json:"sourceProjectPath"`
	BuildProjectPath  string    `json:"buildProjectPath"`
	ArtifactPaths     []string  `json:"artifactPaths,omitempty"`
	FinishedAt        time.Time `json:"finishedAt"`
}

type Runner struct {
	cfg     config.Config
	mu      sync.Mutex
	running map[string]struct{}
}

func NewRunner(cfg config.Config) *Runner {
	return &Runner{
		cfg:     cfg,
		running: make(map[string]struct{}),
	}
}

func (r *Runner) Run(ctx context.Context, req Request, onStarted func(Started) error, onOutput func(Output) error) (Complete, error) {
	sourceDir, err := r.cfg.ResolveProjectDir(req.ProjectPath)
	if err != nil {
		return Complete{}, err
	}
	if err := validateProjectDir(sourceDir); err != nil {
		return Complete{}, err
	}

	release, err := r.acquire(sourceDir)
	if err != nil {
		return Complete{}, err
	}
	defer release()

	mode, err := normalizeMode(req.Mode)
	if err != nil {
		return Complete{}, err
	}

	buildDir := sourceDir
	if mode == ModeSafe {
		buildDir, err = r.prepareSafeWorkspace(sourceDir, onOutput)
		if err != nil {
			return Complete{
				Mode:              mode,
				Status:            "failed",
				ExitCode:          -1,
				SourceProjectPath: sourceDir,
				BuildProjectPath:  buildDir,
				FinishedAt:        time.Now().UTC(),
			}, err
		}

		installCommand := shellInstallCommand(r.cfg.NPMBin, buildDir)
		if installCommand != "" {
			_ = emitOutput(onOutput, "system", "Running "+installCommand)
			installComplete, installErr := r.runShellCommand(ctx, mode, sourceDir, buildDir, installCommand, nil, onOutput)
			if installErr != nil {
				installComplete.SourceProjectPath = sourceDir
				installComplete.BuildProjectPath = buildDir
				installComplete.ArtifactPaths = discoverArtifactPaths(buildDir)
				return installComplete, installErr
			}
		}
	}

	buildCommand := shellBuildCommand(r.cfg.NPMBin)
	complete, err := r.runShellCommand(ctx, mode, sourceDir, buildDir, buildCommand, onStarted, onOutput)
	complete.ArtifactPaths = discoverArtifactPaths(buildDir)
	return complete, err
}

func (r *Runner) acquire(sourceDir string) (func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.running[sourceDir]; ok {
		return nil, fmt.Errorf("build already running for %s", sourceDir)
	}
	r.running[sourceDir] = struct{}{}

	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.running, sourceDir)
	}, nil
}

func (r *Runner) prepareSafeWorkspace(sourceDir string, onOutput func(Output) error) (string, error) {
	buildRoot := strings.TrimSpace(r.cfg.BuildRootDir)
	if buildRoot == "" {
		buildRoot = "/tmp/heya-builds"
	}
	if err := os.MkdirAll(buildRoot, 0o755); err != nil {
		return "", fmt.Errorf("create build root: %w", err)
	}

	buildDir, err := os.MkdirTemp(buildRoot, safeName(filepath.Base(sourceDir))+"-*")
	if err != nil {
		return "", fmt.Errorf("create safe build workspace: %w", err)
	}

	_ = emitOutput(onOutput, "system", "Copying project to "+buildDir)
	if err := copyProject(sourceDir, buildDir); err != nil {
		return buildDir, fmt.Errorf("copy project into safe build workspace: %w", err)
	}
	return buildDir, nil
}

func (r *Runner) runShellCommand(ctx context.Context, mode string, sourceDir string, buildDir string, command string, onStarted func(Started) error, onOutput func(Output) error) (Complete, error) {
	cmd := exec.Command(commandShell(r.cfg.CommandShell), shellArgs(r.cfg.CommandShell, command)...)
	cmd.Dir = buildDir
	cmd.Env = shellEnvironment(os.Environ())
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Complete{}, fmt.Errorf("create stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return Complete{}, fmt.Errorf("create stderr pipe: %w", err)
	}

	startedAt := time.Now().UTC()
	if err := cmd.Start(); err != nil {
		return Complete{}, fmt.Errorf("start build command: %w", err)
	}
	waitDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = terminateProcessGroup(cmd.Process.Pid)
		case <-waitDone:
		}
	}()

	if onStarted != nil {
		if err := onStarted(Started{
			Mode:              mode,
			SourceProjectPath: sourceDir,
			BuildProjectPath:  buildDir,
			Command:           command,
			PID:               strconv.Itoa(cmd.Process.Pid),
			StartedAt:         startedAt,
		}); err != nil {
			_ = terminateProcessGroup(cmd.Process.Pid)
			_ = cmd.Wait()
			close(waitDone)
			return Complete{}, err
		}
	}

	outputErr := make(chan error, 2)
	go streamOutput(stdout, "stdout", onOutput, outputErr)
	go streamOutput(stderr, "stderr", onOutput, outputErr)

	waitErr := cmd.Wait()
	close(waitDone)
	<-outputErr
	<-outputErr

	finishedAt := time.Now().UTC()
	exitCode := 0
	status := "success"
	if waitErr != nil {
		status = "failed"
		exitCode = exitCodeFromError(waitErr)
	}
	if ctx.Err() != nil {
		status = "canceled"
	}

	complete := Complete{
		Mode:              mode,
		Status:            status,
		ExitCode:          exitCode,
		SourceProjectPath: sourceDir,
		BuildProjectPath:  buildDir,
		FinishedAt:        finishedAt,
	}
	return complete, waitErr
}

func streamOutput(reader io.Reader, stream string, onOutput func(Output) error, done chan<- error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if onOutput == nil {
			continue
		}
		if err := onOutput(Output{
			Stream: stream,
			Data:   scanner.Text(),
		}); err != nil {
			done <- err
			return
		}
	}
	done <- scanner.Err()
}

func copyProject(sourceDir, buildDir string) error {
	return filepath.WalkDir(sourceDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		if shouldExclude(rel, entry) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		dest := filepath.Join(buildDir, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}

		switch {
		case entry.Type()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(target, dest)
		case entry.IsDir():
			return os.MkdirAll(dest, info.Mode().Perm())
		case entry.Type().IsRegular():
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				return err
			}
			return copyFile(path, dest, info.Mode().Perm())
		default:
			return nil
		}
	})
}

func copyFile(source, dest string, perm fs.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func shouldExclude(rel string, entry fs.DirEntry) bool {
	name := entry.Name()
	if rel == ".DS_Store" || strings.HasSuffix(rel, string(filepath.Separator)+".DS_Store") {
		return true
	}
	if !entry.IsDir() {
		return false
	}

	switch name {
	case ".git", "node_modules", ".output", "dist", ".vinxi", ".nitro", "test-results", "tmp":
		return true
	default:
		return false
	}
}

func discoverArtifactPaths(buildDir string) []string {
	var paths []string
	for _, name := range []string{".output", "dist", "build"} {
		path := filepath.Join(buildDir, name)
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			paths = append(paths, path)
		}
	}
	return paths
}

func exitCodeFromError(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
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

func normalizeMode(mode string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return ModeSafe, nil
	}
	switch mode {
	case ModeSafe, ModeLive:
		return mode, nil
	default:
		return "", config.ValidationError("mode must be safe or live")
	}
}

func shellInstallCommand(npmBin string, projectDir string) string {
	npmBin = strings.TrimSpace(npmBin)
	if npmBin == "" {
		npmBin = "npm"
	}
	if _, err := os.Stat(filepath.Join(projectDir, "package-lock.json")); err == nil {
		return shellQuote(npmBin) + " ci"
	}
	return shellQuote(npmBin) + " install"
}

func shellBuildCommand(npmBin string) string {
	npmBin = strings.TrimSpace(npmBin)
	if npmBin == "" {
		npmBin = "npm"
	}
	return shellQuote(npmBin) + " run build"
}

func shellArgs(shellPath, command string) []string {
	switch filepath.Base(shellPath) {
	case "zsh", "bash":
		return []string{"-lc", command}
	default:
		return []string{"-c", command}
	}
}

func commandShell(shellPath string) string {
	shellPath = strings.TrimSpace(shellPath)
	if shellPath == "" {
		return "/bin/zsh"
	}
	return shellPath
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

func safeName(value string) string {
	value = regexp.MustCompile(`[^a-zA-Z0-9._-]+`).ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return "build"
	}
	return value
}

func emitOutput(onOutput func(Output) error, stream string, data string) error {
	if onOutput == nil {
		return nil
	}
	return onOutput(Output{Stream: stream, Data: data})
}

func terminateProcessGroup(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
