package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr                string
	ProjectBaseDir          string
	DefaultProjectDir       string
	DefaultDevPort          int
	DevServerScheme         string
	DevServerHost           string
	DevServerBindHost       string
	DevReadyHost            string
	WebSocketAllowedOrigins []string
	LogDir                  string
	BuildRootDir            string
	AccountInfoURL          string
	AccountInfoToken        string
	AccountInfoTimeout      time.Duration
	NPMBin                  string
	CommandShell            string
	DevReadyTimeout         time.Duration
	DevIdleTimeout          time.Duration
	ProcessStopTimeout      time.Duration
}

func Load() (Config, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return Config{}, fmt.Errorf("get working directory: %w", err)
	}

	defaultBaseDir := filepath.Dir(cwd)
	projectBaseDir, err := absClean(envString("HEYA_PROJECT_BASE_DIR", defaultBaseDir))
	if err != nil {
		return Config{}, fmt.Errorf("invalid HEYA_PROJECT_BASE_DIR: %w", err)
	}

	defaultProjectDir := envString("HEYA_DEFAULT_PROJECT_DIR", "/Library/WebServer/Documents/abc/storage/app/frontend")
	if defaultProjectDir != "" {
		defaultProjectDir, err = resolveProjectPath(projectBaseDir, defaultProjectDir)
		if err != nil {
			return Config{}, fmt.Errorf("invalid HEYA_DEFAULT_PROJECT_DIR: %w", err)
		}
	}

	defaultPort, err := envInt("HEYA_DEFAULT_DEV_PORT", 3002)
	if err != nil {
		return Config{}, err
	}
	if !validPort(defaultPort) {
		return Config{}, fmt.Errorf("HEYA_DEFAULT_DEV_PORT must be between 1 and 65535")
	}

	processStopTimeout, err := envDuration("HEYA_PROCESS_STOP_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	devIdleTimeout, err := envDuration("HEYA_DEV_IDLE_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	devReadyTimeout, err := envDuration("HEYA_DEV_READY_TIMEOUT", 60*time.Second)
	if err != nil {
		return Config{}, err
	}
	accountInfoTimeout, err := envDuration("HEYA_ACCOUNT_INFO_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	webSocketAllowedOrigins, err := envOriginList("HEYA_WEBSOCKET_ALLOWED_ORIGINS", []string{
		"https://admin.thewebaddicts.com",
	})
	if err != nil {
		return Config{}, err
	}

	return Config{
		HTTPAddr:                envString("HEYA_HTTP_ADDR", ":8998"),
		ProjectBaseDir:          projectBaseDir,
		DefaultProjectDir:       defaultProjectDir,
		DefaultDevPort:          defaultPort,
		DevServerScheme:         envString("HEYA_DEV_SERVER_SCHEME", "http"),
		DevServerHost:           envString("HEYA_DEV_SERVER_HOST", "localhost"),
		DevServerBindHost:       envString("HEYA_DEV_SERVER_BIND_HOST", envString("DEV_SERVER_HOST", "0.0.0.0")),
		DevReadyHost:            envString("HEYA_DEV_READY_HOST", "localhost"),
		WebSocketAllowedOrigins: webSocketAllowedOrigins,
		LogDir:                  envString("HEYA_LOG_DIR", "/tmp/heya-solidjs-manager/logs"),
		BuildRootDir:            envString("HEYA_BUILD_ROOT_DIR", "/tmp/heya-builds"),
		AccountInfoURL:          envString("HEYA_ACCOUNT_INFO_URL", "https://devops.twalab.live/api/v2/theme-builder/account/info"),
		AccountInfoToken:        envString("HEYA_ACCOUNT_INFO_TOKEN", "QqJ1bbRZ2KIXrqcKb1lyxxa79wYx8IbtvxXBXv1y1uyOfjbSZU282eLgscQ1ix3Z"),
		AccountInfoTimeout:      accountInfoTimeout,
		NPMBin:                  envString("HEYA_NPM_BIN", "npm"),
		CommandShell:            envString("HEYA_COMMAND_SHELL", envString("SHELL", "/bin/zsh")),
		DevReadyTimeout:         devReadyTimeout,
		DevIdleTimeout:          devIdleTimeout,
		ProcessStopTimeout:      processStopTimeout,
	}, nil
}

func (c Config) ResolveProjectDir(projectPath string) (string, error) {
	projectPath = strings.TrimSpace(projectPath)
	if projectPath == "" {
		projectPath = c.DefaultProjectDir
	}
	if projectPath == "" {
		return "", ValidationError("projectPath is required unless HEYA_DEFAULT_PROJECT_DIR is set")
	}

	resolved, err := resolveProjectPath(c.ProjectBaseDir, projectPath)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func (c Config) DevServerURL(port int) string {
	scheme := strings.TrimSpace(c.DevServerScheme)
	if scheme == "" {
		scheme = "http"
	}

	host := strings.TrimSpace(c.DevServerHost)
	if host == "" {
		host = "localhost"
	}

	return fmt.Sprintf("%s://%s:%d", scheme, host, port)
}

type ValidationError string

func (e ValidationError) Error() string {
	return string(e)
}

func resolveProjectPath(baseDir, projectPath string) (string, error) {
	var resolved string
	if filepath.IsAbs(projectPath) {
		resolved = filepath.Clean(projectPath)
	} else {
		resolved = filepath.Join(baseDir, projectPath)
	}

	if baseDir == "" {
		return resolved, nil
	}

	rel, err := filepath.Rel(baseDir, resolved)
	if err != nil {
		return "", fmt.Errorf("resolve project path relative to base directory: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", ValidationError("projectPath must stay inside HEYA_PROJECT_BASE_DIR")
	}

	return resolved, nil
}

func absClean(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", ValidationError("path cannot be empty")
	}
	return filepath.Abs(filepath.Clean(path))
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return value, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration such as 10s or 1m: %w", key, err)
	}
	return value, nil
}

func envOriginList(key string, fallback []string) ([]string, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return append([]string(nil), fallback...), nil
	}

	var origins []string
	for _, part := range strings.Split(raw, ",") {
		origin, err := normalizeOrigin(part)
		if err != nil {
			return nil, fmt.Errorf("%s contains invalid origin %q: %w", key, strings.TrimSpace(part), err)
		}
		origins = append(origins, origin)
	}
	return origins, nil
}

func normalizeOrigin(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ValidationError("origin cannot be empty")
	}

	originURL, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if originURL.Scheme == "" || originURL.Host == "" {
		return "", ValidationError("origin must include scheme and host")
	}
	scheme := strings.ToLower(originURL.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", ValidationError("origin scheme must be http or https")
	}
	if originURL.User != nil || originURL.RawQuery != "" || originURL.Fragment != "" {
		return "", ValidationError("origin must not include user info, query, or fragment")
	}
	if originURL.Path != "" && originURL.Path != "/" {
		return "", ValidationError("origin must not include a path")
	}

	return scheme + "://" + strings.ToLower(originURL.Host), nil
}

func validPort(port int) bool {
	return port > 0 && port <= 65535
}
