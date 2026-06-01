package account

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"heya-golang-microservice/internal/config"
)

type Resolver struct {
	cfg    config.Config
	client *http.Client
	logger *slog.Logger
}

type ProjectInfo struct {
	ProjectUser          string
	AccountID            int
	AccountUUID          string
	AccountUsername      string
	AccountLabel         string
	ServerIP             string
	WorkingDirectory     string
	WorkingDirectoryHeya string
	DevPort              int
}

type infoResponse struct {
	Account struct {
		ID          int    `json:"id"`
		UUID        string `json:"uuid"`
		Username    string `json:"username"`
		Label       string `json:"label"`
		PortDevLive *int   `json:"port_dev_live"`
	} `json:"account"`
	ServerIP             string `json:"server_ip"`
	WorkingDirectory     string `json:"working_directory"`
	WorkingDirectoryHeya string `json:"working_directory_heya"`
}

func NewResolver(cfg config.Config, logger *slog.Logger) *Resolver {
	timeout := cfg.AccountInfoTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Resolver{
		cfg: cfg,
		client: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (r *Resolver) Resolve(ctx context.Context, projectUser string) (ProjectInfo, error) {
	projectUser = strings.TrimSpace(projectUser)
	if projectUser == "" {
		return ProjectInfo{}, config.ValidationError("projectUser is required")
	}

	endpoint := strings.TrimSpace(r.cfg.AccountInfoURL)
	if endpoint == "" {
		return ProjectInfo{}, config.ValidationError("HEYA_ACCOUNT_INFO_URL is required when projectUser is used")
	}
	token := strings.TrimSpace(r.cfg.AccountInfoToken)
	if token == "" {
		return ProjectInfo{}, config.ValidationError("HEYA_ACCOUNT_INFO_TOKEN is required when projectUser is used")
	}

	payload, err := json.Marshal(map[string]string{"account": projectUser})
	if err != nil {
		return ProjectInfo{}, fmt.Errorf("encode account info request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return ProjectInfo{}, fmt.Errorf("create account info request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("token", token)

	startedAt := time.Now()
	r.logInfo("account info request",
		"method", http.MethodPost,
		"url", endpoint,
		"projectUser", projectUser,
		"tokenHeaderSet", true,
	)

	resp, err := r.client.Do(req)
	if err != nil {
		r.logError("account info request failed", "projectUser", projectUser, "error", err)
		return ProjectInfo{}, fmt.Errorf("request account info for %q: %w", projectUser, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return ProjectInfo{}, fmt.Errorf("read account info response: %w", err)
	}

	r.logInfo("account info response",
		"projectUser", projectUser,
		"status", resp.StatusCode,
		"duration", time.Since(startedAt),
	)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ProjectInfo{}, fmt.Errorf("account info request failed for %q: status %d: %s", projectUser, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed infoResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return ProjectInfo{}, fmt.Errorf("decode account info response for %q: %w", projectUser, err)
	}

	info := ProjectInfo{
		ProjectUser:          projectUser,
		AccountID:            parsed.Account.ID,
		AccountUUID:          strings.TrimSpace(parsed.Account.UUID),
		AccountUsername:      strings.TrimSpace(parsed.Account.Username),
		AccountLabel:         strings.TrimSpace(parsed.Account.Label),
		ServerIP:             strings.TrimSpace(parsed.ServerIP),
		WorkingDirectory:     strings.TrimSpace(parsed.WorkingDirectory),
		WorkingDirectoryHeya: strings.TrimSpace(parsed.WorkingDirectoryHeya),
	}
	if parsed.Account.PortDevLive != nil {
		info.DevPort = *parsed.Account.PortDevLive
	}
	if info.WorkingDirectoryHeya == "" {
		return ProjectInfo{}, fmt.Errorf("account info response for %q is missing working_directory_heya", projectUser)
	}
	if info.DevPort < 1 || info.DevPort > 65535 {
		return ProjectInfo{}, fmt.Errorf("account info response for %q has invalid port_dev_live %d", projectUser, info.DevPort)
	}

	r.logInfo("account info resolved",
		"projectUser", projectUser,
		"accountID", info.AccountID,
		"accountUsername", info.AccountUsername,
		"accountLabel", info.AccountLabel,
		"serverIP", info.ServerIP,
		"workingDirectory", info.WorkingDirectory,
		"workingDirectoryHeya", info.WorkingDirectoryHeya,
		"portDevLive", info.DevPort,
	)

	return info, nil
}

func (r *Resolver) logInfo(message string, args ...any) {
	if r.logger != nil {
		r.logger.Info(message, args...)
	}
}

func (r *Resolver) logError(message string, args ...any) {
	if r.logger != nil {
		r.logger.Error(message, args...)
	}
}
