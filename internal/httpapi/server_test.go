package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"heya-golang-microservice/internal/config"
	"heya-golang-microservice/internal/dev"
)

type fakeRunner struct {
	mu        sync.Mutex
	runCount  int
	stopCount int
	lastReq   dev.RunRequest
}

func (r *fakeRunner) Run(_ context.Context, req dev.RunRequest) (dev.RunResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.runCount++
	r.lastReq = req
	projectPath := req.ProjectPath
	if projectPath == "" {
		projectPath = "/tmp/solid-app"
	}
	port := req.Port
	if port == 0 {
		port = 3002
	}
	host := req.DevServerHost
	if host == "" {
		host = "localhost"
	}
	return dev.RunResult{
		ProjectPath:       projectPath,
		Port:              port,
		DevServerURL:      fmt.Sprintf("http://%s:%d", host, port),
		DevServerBasePath: req.DevServerBasePath,
		PID:               "12345",
		LogFile:           "/tmp/heya.log",
		Command:           fmt.Sprintf("npm run dev -- --port %d", port),
		Target:            "127.0.0.1:22",
		StartedAt:         time.Now().UTC(),
	}, nil
}

func (r *fakeRunner) Stop(context.Context, dev.RunResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopCount++
	return nil
}

func (r *fakeRunner) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.runCount, r.stopCount
}

func (r *fakeRunner) lastRequest() dev.RunRequest {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lastReq
}

func TestDevRunWebSocketReusesServerUntilLastDisconnect(t *testing.T) {
	projectDir := t.TempDir()
	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     projectDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		DevIdleTimeout:     20 * time.Millisecond,
		ProcessStopTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/dev/run"
	first := dialWebSocket(t, wsURL)
	defer first.Close()
	second := dialWebSocket(t, wsURL)
	defer second.Close()

	firstMessage := readWebSocketMessage(t, first)
	secondMessage := readWebSocketMessage(t, second)

	if firstMessage.DevServerURL != "http://localhost:3002" {
		t.Fatalf("first devServerURL = %q, want %q", firstMessage.DevServerURL, "http://localhost:3002")
	}
	if firstMessage.Connections != 1 {
		t.Fatalf("first connections = %d, want 1", firstMessage.Connections)
	}
	if secondMessage.Connections != 2 {
		t.Fatalf("second connections = %d, want 2", secondMessage.Connections)
	}

	runCount, stopCount := runner.counts()
	if runCount != 1 {
		t.Fatalf("runCount = %d, want 1", runCount)
	}
	if stopCount != 0 {
		t.Fatalf("stopCount = %d, want 0", stopCount)
	}

	_ = first.Close()
	waitForCounts(t, runner, 1, 0)

	_ = second.Close()
	waitForCounts(t, runner, 1, 1)
}

func TestDevRunWebSocketResolvesProjectUser(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"dev":"vite"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("token"); got != "test-token" {
			t.Fatalf("token header = %q, want test-token", got)
		}

		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if payload["account"] != "energy-user" {
			t.Fatalf("account body = %q, want energy-user", payload["account"])
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": 12017,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		DevIdleTimeout:     20 * time.Millisecond,
		ProcessStopTimeout: time.Second,
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	conn := dialWebSocket(t, "ws"+strings.TrimPrefix(testServer.URL, "http")+"/dev/run?projectUser=energy-user")
	defer conn.Close()
	message := readWebSocketMessage(t, conn)

	wantProxyURL := "https://91-98-82-198-heya-service.twalab.cloud/dev/proxy/energy-user/"
	if message.DevServerURL != wantProxyURL {
		t.Fatalf("devServerURL = %q, want %q", message.DevServerURL, wantProxyURL)
	}
	if message.DevProxyURL != wantProxyURL {
		t.Fatalf("devProxyURL = %q, want %q", message.DevProxyURL, wantProxyURL)
	}
	if message.Run.DevServerBasePath != "/dev/proxy/energy-user/" {
		t.Fatalf("run DevServerBasePath = %q, want %q", message.Run.DevServerBasePath, "/dev/proxy/energy-user/")
	}
	if message.Run.DevServerURL != wantProxyURL {
		t.Fatalf("run DevServerURL = %q, want %q", message.Run.DevServerURL, wantProxyURL)
	}
	req := runner.lastRequest()
	if req.ProjectPath != projectDir {
		t.Fatalf("runner ProjectPath = %q, want %q", req.ProjectPath, projectDir)
	}
	if req.Port != 12017 {
		t.Fatalf("runner Port = %d, want 12017", req.Port)
	}
	if req.DevServerHost != "91.98.82.198" {
		t.Fatalf("runner DevServerHost = %q, want 91.98.82.198", req.DevServerHost)
	}
	if req.DevServerPublicHost != "91-98-82-198-heya-service.twalab.cloud" {
		t.Fatalf("runner DevServerPublicHost = %q, want dashed public host", req.DevServerPublicHost)
	}
	if req.DevServerBasePath != "/dev/proxy/energy-user/" {
		t.Fatalf("runner DevServerBasePath = %q, want %q", req.DevServerBasePath, "/dev/proxy/energy-user/")
	}
}

func TestDevRunWebSocketReturnsPreviewPathUnderProxy(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": 12017,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		DevIdleTimeout:     20 * time.Millisecond,
		ProcessStopTimeout: time.Second,
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/dev/run?projectUser=energy-user&previewPath=%2Fthemes%2F57726969-9e2e-11ed-9f8e-42010a960004%2Fz-6a1ef6c3dcca6&preview=true"
	conn := dialWebSocket(t, wsURL)
	defer conn.Close()
	message := readWebSocketMessage(t, conn)

	wantProxyRootURL := "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/"
	wantDevServerURL := wantProxyRootURL + "?preview=true"
	if message.DevServerURL != wantDevServerURL {
		t.Fatalf("devServerURL = %q, want %q", message.DevServerURL, wantDevServerURL)
	}
	if message.DevProxyURL != wantProxyRootURL {
		t.Fatalf("devProxyURL = %q, want %q", message.DevProxyURL, wantProxyRootURL)
	}
	if message.Run.DevServerURL != wantDevServerURL {
		t.Fatalf("run DevServerURL = %q, want %q", message.Run.DevServerURL, wantDevServerURL)
	}
	if message.Run.DevServerBasePath != "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/" {
		t.Fatalf("run DevServerBasePath = %q, want theme base path", message.Run.DevServerBasePath)
	}
}

func TestDevRunWebSocketRewritesPreviewURLUnderProxy(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": 12017,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		DevIdleTimeout:     20 * time.Millisecond,
		ProcessStopTimeout: time.Second,
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	rawPreviewURL := "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6?preview=true"
	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/dev/run?projectUser=energy-user&previewUrl=" + url.QueryEscape(rawPreviewURL)
	conn := dialWebSocket(t, wsURL)
	defer conn.Close()
	message := readWebSocketMessage(t, conn)

	wantDevServerURL := "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/?preview=true"
	if message.DevServerURL != wantDevServerURL {
		t.Fatalf("devServerURL = %q, want %q", message.DevServerURL, wantDevServerURL)
	}
	if strings.Contains(message.DevServerURL, "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/themes/") {
		t.Fatalf("devServerURL = %q, want single theme base path", message.DevServerURL)
	}
}

func TestDevRunWebSocketUsesRefererThemePath(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": 12017,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:          baseDir,
		DefaultProjectDir:       projectDir,
		DefaultDevPort:          3002,
		DevIdleTimeout:          20 * time.Millisecond,
		ProcessStopTimeout:      time.Second,
		AccountInfoURL:          accountServer.URL,
		AccountInfoToken:        "test-token",
		AccountInfoTimeout:      time.Second,
		WebSocketAllowedOrigins: []string{"https://admin.thewebaddicts.com"},
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	conn := dialWebSocketWithHeaders(t, "ws"+strings.TrimPrefix(testServer.URL, "http")+"/dev/run?projectUser=energy-user", http.Header{
		"Origin":  []string{"https://admin.thewebaddicts.com"},
		"Referer": []string{"https://admin.thewebaddicts.com/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6?path=%2F"},
	})
	defer conn.Close()
	message := readWebSocketMessage(t, conn)

	wantProxyRootURL := "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/"
	if message.DevProxyURL != wantProxyRootURL {
		t.Fatalf("devProxyURL = %q, want %q", message.DevProxyURL, wantProxyRootURL)
	}
	if message.Run.DevServerBasePath != "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/" {
		t.Fatalf("run DevServerBasePath = %q, want theme base path", message.Run.DevServerBasePath)
	}
}

func TestDevRunWebSocketAllowsConfiguredOrigin(t *testing.T) {
	projectDir := t.TempDir()
	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:          projectDir,
		DefaultProjectDir:       projectDir,
		DefaultDevPort:          3002,
		DevIdleTimeout:          20 * time.Millisecond,
		ProcessStopTimeout:      time.Second,
		WebSocketAllowedOrigins: []string{"https://admin.thewebaddicts.com"},
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	conn := dialWebSocketWithHeaders(t, "ws"+strings.TrimPrefix(testServer.URL, "http")+"/dev/run", http.Header{
		"Origin": []string{"https://admin.thewebaddicts.com"},
	})
	defer conn.Close()

	message := readWebSocketMessage(t, conn)
	if message.DevServerURL != "http://localhost:3002" {
		t.Fatalf("devServerURL = %q, want %q", message.DevServerURL, "http://localhost:3002")
	}
}

func TestDevProxyProxiesProjectUserToLocalDevServer(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}

	var upstreamPath string
	var upstreamQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		upstreamQuery = r.URL.RawQuery
		w.Header().Set("X-Upstream", "vite")
		_, _ = w.Write([]byte("proxied dev server"))
	}))
	defer upstream.Close()
	upstreamPort := serverPort(t, upstream.URL)

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": upstreamPort,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		DevReadyHost:       "127.0.0.1",
		DevIdleTimeout:     20 * time.Millisecond,
		ProcessStopTimeout: time.Second,
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	resp, err := http.Get(testServer.URL + "/dev/proxy/energy-user/hello?foo=bar&projectUser=ignored")
	if err != nil {
		t.Fatalf("GET dev proxy: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, http.StatusOK, string(body))
	}
	if string(body) != "proxied dev server" {
		t.Fatalf("body = %q, want proxied dev server", string(body))
	}
	if resp.Header.Get("X-Upstream") != "vite" {
		t.Fatalf("X-Upstream = %q, want vite", resp.Header.Get("X-Upstream"))
	}
	if upstreamPath != "/hello" {
		t.Fatalf("upstream path = %q, want %q", upstreamPath, "/hello")
	}
	if upstreamQuery != "foo=bar" {
		t.Fatalf("upstream query = %q, want %q", upstreamQuery, "foo=bar")
	}

	req := runner.lastRequest()
	if req.ProjectPath != projectDir {
		t.Fatalf("runner ProjectPath = %q, want %q", req.ProjectPath, projectDir)
	}
	if req.Port != upstreamPort {
		t.Fatalf("runner Port = %d, want %d", req.Port, upstreamPort)
	}
	if req.DevServerBasePath != "/dev/proxy/energy-user/" {
		t.Fatalf("runner DevServerBasePath = %q, want %q", req.DevServerBasePath, "/dev/proxy/energy-user/")
	}
}

func TestThemeProxyProxiesRegisteredRouteToLocalDevServer(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}

	var upstreamPath string
	var upstreamQuery string
	var upstreamHost string
	var upstreamForwardedHost string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		upstreamQuery = r.URL.RawQuery
		upstreamHost = r.Host
		upstreamForwardedHost = r.Header.Get("X-Forwarded-Host")
		w.Header().Set("X-Upstream", "vite")
		_, _ = w.Write([]byte("proxied theme dev server"))
	}))
	defer upstream.Close()
	upstreamPort := serverPort(t, upstream.URL)

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": upstreamPort,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		DevReadyHost:       "127.0.0.1",
		DevIdleTimeout:     time.Second,
		ProcessStopTimeout: time.Second,
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/dev/run?projectUser=energy-user&previewPath=%2Fthemes%2F57726969-9e2e-11ed-9f8e-42010a960004%2Fz-6a1ef6c3dcca6&preview=true"
	conn := dialWebSocket(t, wsURL)
	defer conn.Close()
	message := readWebSocketMessage(t, conn)
	if message.DevProxyURL != "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/" {
		t.Fatalf("devProxyURL = %q, want route-native theme root", message.DevProxyURL)
	}

	reqHTTP, err := http.NewRequest(http.MethodGet, testServer.URL+"/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/@vite/client?foo=bar", nil)
	if err != nil {
		t.Fatalf("create theme proxy request: %v", err)
	}
	reqHTTP.Host = "91-98-82-198-heya-service.twalab.cloud"

	resp, err := http.DefaultClient.Do(reqHTTP)
	if err != nil {
		t.Fatalf("GET theme proxy: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, http.StatusOK, string(body))
	}
	if string(body) != "proxied theme dev server" {
		t.Fatalf("body = %q, want proxied theme dev server", string(body))
	}
	if resp.Header.Get("X-Upstream") != "vite" {
		t.Fatalf("X-Upstream = %q, want vite", resp.Header.Get("X-Upstream"))
	}
	if upstreamPath != "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/@vite/client" {
		t.Fatalf("upstream path = %q, want original theme path", upstreamPath)
	}
	if upstreamQuery != "foo=bar" {
		t.Fatalf("upstream query = %q, want %q", upstreamQuery, "foo=bar")
	}
	wantUpstreamHost := net.JoinHostPort("127.0.0.1", strconv.Itoa(upstreamPort))
	if upstreamHost != wantUpstreamHost {
		t.Fatalf("upstream host = %q, want local target host %q", upstreamHost, wantUpstreamHost)
	}
	if upstreamForwardedHost != "91-98-82-198-heya-service.twalab.cloud" {
		t.Fatalf("X-Forwarded-Host = %q, want public proxy host", upstreamForwardedHost)
	}

	req := runner.lastRequest()
	if req.DevServerBasePath != "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/" {
		t.Fatalf("runner DevServerBasePath = %q, want theme base path", req.DevServerBasePath)
	}
}

func TestThemeProxyRewritesBuildAssetURLsAndStripsAssetBasePath(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}

	var upstreamPath string
	var upstreamQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		upstreamQuery = r.URL.RawQuery
		switch r.URL.Path {
		case "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/":
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(`<script type="module" src="/_build/@vite/client"></script><script type="module" src="/_build/@fs/app/src/entry-client.tsx"></script>`))
		case "/_build/@vite/client":
			w.Header().Set("Content-Type", "application/javascript")
			_, _ = w.Write([]byte("import \"/_build/chunk.js\"; import.meta.env = {\"BASE_URL\":\"/_build\"}; const api = `/api/theme-page/by-file`; const hmrPort = 38123;"))
		case "/_build/@fs/home/energy-user/app/src/routes/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/index.tsx":
			w.Header().Set("Content-Type", "application/javascript")
			_, _ = w.Write([]byte(`export default function Page() {}`))
		case "/api/theme-page/by-file":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/api/theme-watch/version":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"version":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()
	upstreamPort := serverPort(t, upstream.URL)

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": upstreamPort,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		DevReadyHost:       "127.0.0.1",
		DevIdleTimeout:     time.Second,
		ProcessStopTimeout: time.Second,
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/dev/run?projectUser=energy-user&previewPath=%2Fthemes%2F57726969-9e2e-11ed-9f8e-42010a960004%2Fz-6a1ef6c3dcca6%2F&preview=true"
	conn := dialWebSocket(t, wsURL)
	defer conn.Close()
	_ = readWebSocketMessage(t, conn)

	reqHTML, err := http.NewRequest(http.MethodGet, testServer.URL+"/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/", nil)
	if err != nil {
		t.Fatalf("create theme HTML request: %v", err)
	}
	reqHTML.Host = "91-98-82-198-heya-service.twalab.cloud"
	respHTML, err := http.DefaultClient.Do(reqHTML)
	if err != nil {
		t.Fatalf("GET theme HTML: %v", err)
	}
	defer respHTML.Body.Close()
	htmlBody, err := io.ReadAll(respHTML.Body)
	if err != nil {
		t.Fatalf("read HTML body: %v", err)
	}
	html := string(htmlBody)
	if !strings.Contains(html, `src="/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/_build/@vite/client"`) {
		t.Fatalf("HTML did not rewrite vite client asset URL: %s", html)
	}
	if !strings.Contains(html, `src="/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/_build/@fs/app/src/entry-client.tsx"`) {
		t.Fatalf("HTML did not rewrite entry client asset URL: %s", html)
	}

	reqJS, err := http.NewRequest(http.MethodGet, testServer.URL+"/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/_build/@vite/client", nil)
	if err != nil {
		t.Fatalf("create theme JS request: %v", err)
	}
	reqJS.Host = "91-98-82-198-heya-service.twalab.cloud"
	respJS, err := http.DefaultClient.Do(reqJS)
	if err != nil {
		t.Fatalf("GET theme JS: %v", err)
	}
	defer respJS.Body.Close()
	jsBody, err := io.ReadAll(respJS.Body)
	if err != nil {
		t.Fatalf("read JS body: %v", err)
	}
	if upstreamPath != "/_build/@vite/client" {
		t.Fatalf("upstream path = %q, want %q", upstreamPath, "/_build/@vite/client")
	}
	js := string(jsBody)
	if !strings.Contains(js, `"/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/_build/chunk.js"`) {
		t.Fatalf("JS did not rewrite nested build asset URL: %s", js)
	}
	if !strings.Contains(js, `"BASE_URL":"/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/_build"`) {
		t.Fatalf("JS did not rewrite exact build base URL: %s", js)
	}
	if !strings.Contains(js, "`/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/api/theme-page/by-file`") {
		t.Fatalf("JS did not rewrite backtick theme API URL: %s", js)
	}
	if !strings.Contains(js, `const hmrPort = importMetaUrl.port || (importMetaUrl.protocol === "https:" ? 443 : 80);`) {
		t.Fatalf("JS did not rewrite Vite HMR port: %s", js)
	}

	reqAPI, err := http.NewRequest(http.MethodGet, testServer.URL+"/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/api/theme-page/by-file", nil)
	if err != nil {
		t.Fatalf("create theme API request: %v", err)
	}
	reqAPI.Host = "91-98-82-198-heya-service.twalab.cloud"
	respAPI, err := http.DefaultClient.Do(reqAPI)
	if err != nil {
		t.Fatalf("GET theme API: %v", err)
	}
	defer respAPI.Body.Close()
	if upstreamPath != "/api/theme-page/by-file" {
		t.Fatalf("upstream path = %q, want %q", upstreamPath, "/api/theme-page/by-file")
	}

	reqWatchAPI, err := http.NewRequest(http.MethodGet, testServer.URL+"/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/api/theme-watch/version?themeUuid=57726969-9e2e-11ed-9f8e-42010a960004&installationId=z-6a1ef6c3dcca6&_=1780438674941", nil)
	if err != nil {
		t.Fatalf("create theme watch API request: %v", err)
	}
	reqWatchAPI.Host = "91-98-82-198-heya-service.twalab.cloud"
	respWatchAPI, err := http.DefaultClient.Do(reqWatchAPI)
	if err != nil {
		t.Fatalf("GET theme watch API: %v", err)
	}
	defer respWatchAPI.Body.Close()
	if upstreamPath != "/api/theme-watch/version" {
		t.Fatalf("upstream path = %q, want %q", upstreamPath, "/api/theme-watch/version")
	}
	if upstreamQuery != "themeUuid=57726969-9e2e-11ed-9f8e-42010a960004&installationId=z-6a1ef6c3dcca6&_=1780438674941" {
		t.Fatalf("upstream query = %q, want theme watch query", upstreamQuery)
	}

	reqRootBuild, err := http.NewRequest(http.MethodGet, testServer.URL+"/_build/@fs/home/energy-user/app/src/routes/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/index.tsx?import&pick=default&pick=$css", nil)
	if err != nil {
		t.Fatalf("create root build request: %v", err)
	}
	reqRootBuild.Host = "91-98-82-198-heya-service.twalab.cloud"
	reqRootBuild.Header.Set("Referer", "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6?preview=true")
	respRootBuild, err := http.DefaultClient.Do(reqRootBuild)
	if err != nil {
		t.Fatalf("GET root build request: %v", err)
	}
	defer respRootBuild.Body.Close()
	if respRootBuild.StatusCode != http.StatusOK {
		t.Fatalf("root build status = %d, want %d", respRootBuild.StatusCode, http.StatusOK)
	}
	if upstreamPath != "/_build/@fs/home/energy-user/app/src/routes/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/index.tsx" {
		t.Fatalf("upstream path = %q, want root build path", upstreamPath)
	}
	if upstreamQuery != "import&pick=default&pick=$css" {
		t.Fatalf("upstream query = %q, want root build query", upstreamQuery)
	}

	reqRootBuildNoReferer, err := http.NewRequest(http.MethodGet, testServer.URL+"/_build/@vite/client", nil)
	if err != nil {
		t.Fatalf("create root build no-referer request: %v", err)
	}
	reqRootBuildNoReferer.Host = "91-98-82-198-heya-service.twalab.cloud"
	respRootBuildNoReferer, err := http.DefaultClient.Do(reqRootBuildNoReferer)
	if err != nil {
		t.Fatalf("GET root build no-referer request: %v", err)
	}
	defer respRootBuildNoReferer.Body.Close()
	if respRootBuildNoReferer.StatusCode != http.StatusOK {
		t.Fatalf("root build no-referer status = %d, want %d", respRootBuildNoReferer.StatusCode, http.StatusOK)
	}

	reqRootWatchAPI, err := http.NewRequest(http.MethodGet, testServer.URL+"/api/theme-watch/version?themeUuid=57726969-9e2e-11ed-9f8e-42010a960004&installationId=z-6a1ef6c3dcca6&_=1780438674941", nil)
	if err != nil {
		t.Fatalf("create root theme watch API request: %v", err)
	}
	reqRootWatchAPI.Host = "91-98-82-198-heya-service.twalab.cloud"
	reqRootWatchAPI.Header.Set("Referer", "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6?preview=true")
	respRootWatchAPI, err := http.DefaultClient.Do(reqRootWatchAPI)
	if err != nil {
		t.Fatalf("GET root theme watch API request: %v", err)
	}
	defer respRootWatchAPI.Body.Close()
	if respRootWatchAPI.StatusCode != http.StatusOK {
		t.Fatalf("root theme watch API status = %d, want %d", respRootWatchAPI.StatusCode, http.StatusOK)
	}
	if upstreamPath != "/api/theme-watch/version" {
		t.Fatalf("upstream path = %q, want %q", upstreamPath, "/api/theme-watch/version")
	}
}

func TestDevRunWebSocketRejectsThemeProxyConflict(t *testing.T) {
	baseDir := t.TempDir()
	projectDirOne := filepath.Join(baseDir, "account-one", "storage", "app", "frontend")
	projectDirTwo := filepath.Join(baseDir, "account-two", "storage", "app", "frontend")
	for _, projectDir := range []string{projectDirOne, projectDirTwo} {
		if err := os.MkdirAll(projectDir, 0o755); err != nil {
			t.Fatalf("create project dir: %v", err)
		}
	}

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}

		projectDir := projectDirOne
		port := 12017
		if payload["account"] == "other-user" {
			projectDir = projectDirTwo
			port = 12018
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      payload["account"],
				"label":         "Energy Bridge",
				"port_dev_live": port,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDirOne,
		DefaultDevPort:     3002,
		DevIdleTimeout:     time.Second,
		ProcessStopTimeout: time.Second,
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	previewPath := "%2Fthemes%2F57726969-9e2e-11ed-9f8e-42010a960004%2Fz-6a1ef6c3dcca6"
	first := dialWebSocket(t, "ws"+strings.TrimPrefix(testServer.URL, "http")+"/dev/run?projectUser=energy-user&previewPath="+previewPath)
	defer first.Close()
	_ = readWebSocketMessage(t, first)

	secondURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/dev/run?projectUser=other-user&previewPath=" + previewPath
	second, resp, err := websocket.DefaultDialer.Dial(secondURL, nil)
	if err == nil {
		second.Close()
		t.Fatal("second Dial() succeeded, want theme proxy conflict")
	}
	if resp == nil {
		t.Fatal("second Dial() response is nil, want HTTP conflict")
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second Dial() status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}
}

func TestBuildRunWebSocketStreamsBuildOutput(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`console.log("building");`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		DefaultDevPort:    3002,
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	conn := dialWebSocket(t, "ws"+strings.TrimPrefix(testServer.URL, "http")+"/build/run")
	defer conn.Close()

	seenStarted := false
	seenOutput := false
	for {
		var message map[string]any
		if err := conn.ReadJSON(&message); err != nil {
			t.Fatalf("ReadJSON() error = %v", err)
		}

		switch message["type"] {
		case "build_started":
			seenStarted = true
		case "build_output":
			if message["data"] == "building" {
				seenOutput = true
			}
		case "build_complete":
			if message["status"] != "success" {
				t.Fatalf("build_complete status = %v, want success", message["status"])
			}
			if !seenStarted {
				t.Fatal("did not see build_started")
			}
			if !seenOutput {
				t.Fatal("did not see expected build_output")
			}
			return
		}
	}
}

func TestBuildRunWebSocketResolvesProjectUser(t *testing.T) {
	baseDir := t.TempDir()
	projectDir := filepath.Join(baseDir, "account", "storage", "app", "frontend")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`console.log("building project user");`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("token"); got != "test-token" {
			t.Fatalf("token header = %q, want test-token", got)
		}

		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if payload["account"] != "energy-user" {
			t.Fatalf("account body = %q, want energy-user", payload["account"])
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"account": map[string]any{
				"id":            257,
				"uuid":          "account-uuid",
				"username":      "energy-user",
				"label":         "Energy Bridge",
				"port_dev_live": 12017,
			},
			"server_ip":              "91.98.82.198",
			"working_directory":      filepath.Dir(filepath.Dir(filepath.Dir(projectDir))),
			"working_directory_heya": projectDir,
		})
	}))
	defer accountServer.Close()

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:     baseDir,
		DefaultProjectDir:  projectDir,
		DefaultDevPort:     3002,
		NPMBin:             "npm",
		CommandShell:       "/bin/zsh",
		AccountInfoURL:     accountServer.URL,
		AccountInfoToken:   "test-token",
		AccountInfoTimeout: time.Second,
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	conn := dialWebSocket(t, "ws"+strings.TrimPrefix(testServer.URL, "http")+"/build/run?projectUser=energy-user&mode=live")
	defer conn.Close()

	for {
		message := readWebSocketMap(t, conn)
		switch message["type"] {
		case "build_output":
			if message["data"] != "building project user" {
				continue
			}
		case "build_complete":
			if message["status"] != "success" {
				t.Fatalf("build_complete status = %v, want success", message["status"])
			}
			return
		}
	}
}

func TestBuildRunWebSocketWatchAttachesAfterDisconnect(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`console.log("started"); setTimeout(() => { console.log("finished"); }, 500);`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		DefaultDevPort:    3002,
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	baseURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/build/run?mode=live"
	first := dialWebSocket(t, baseURL)

	for {
		message := readWebSocketMap(t, first)
		if message["type"] == "build_started" {
			break
		}
	}
	_ = first.Close()

	second := dialWebSocket(t, baseURL+"&watch=true")
	defer second.Close()

	seenRunningStatus := false
	for {
		message := readWebSocketMap(t, second)
		switch message["type"] {
		case "build_status":
			if message["running"] == true {
				seenRunningStatus = true
			}
		case "build_complete":
			if message["status"] != "success" {
				t.Fatalf("build_complete status = %v, want success", message["status"])
			}
			if !seenRunningStatus {
				t.Fatal("watch connection did not receive running build status")
			}
			return
		}
	}
}

func TestBuildRunWebSocketIdleWatchStaysOpenUntilClientCloses(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "package.json"), []byte(`{"scripts":{"build":"node build.mjs"}}`), 0o644); err != nil {
		t.Fatalf("write package.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectDir, "build.mjs"), []byte(`console.log("building");`), 0o644); err != nil {
		t.Fatalf("write build.mjs: %v", err)
	}

	runner := &fakeRunner{}
	server := NewServer(config.Config{
		ProjectBaseDir:    projectDir,
		DefaultProjectDir: projectDir,
		DefaultDevPort:    3002,
		NPMBin:            "npm",
		CommandShell:      "/bin/zsh",
	}, runner, slog.Default())
	testServer := httptest.NewServer(server.Routes())
	defer testServer.Close()

	conn := dialWebSocket(t, "ws"+strings.TrimPrefix(testServer.URL, "http")+"/build/run?watch=true")
	defer conn.Close()

	message := readWebSocketMap(t, conn)
	if message["type"] != "build_status" || message["status"] != "idle" {
		t.Fatalf("message = %#v, want idle build_status", message)
	}

	if err := conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	_, _, err := conn.NextReader()
	if err == nil {
		t.Fatal("NextReader() error = nil, want timeout while idle watch stays open")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("NextReader() error = %v, want timeout", err)
	}
}

func TestWebSocketOriginValidation(t *testing.T) {
	tests := []struct {
		name           string
		requestHost    string
		origin         string
		allowedOrigins []string
		want           bool
	}{
		{
			name:        "missing origin",
			requestHost: "91-98-82-198-heya-service.twalab.cloud",
			want:        true,
		},
		{
			name:        "same host",
			requestHost: "91-98-82-198-heya-service.twalab.cloud",
			origin:      "https://91-98-82-198-heya-service.twalab.cloud",
			want:        true,
		},
		{
			name:        "localhost dev origin",
			requestHost: "91-98-82-198-heya-service.twalab.cloud",
			origin:      "http://localhost:5173",
			want:        true,
		},
		{
			name:        "localhost dev origin is case insensitive",
			requestHost: "91-98-82-198-heya-service.twalab.cloud",
			origin:      "http://LOCALHOST:5173",
			want:        true,
		},
		{
			name:           "configured admin origin",
			requestHost:    "91-98-82-198-heya-service.twalab.cloud",
			origin:         "https://admin.thewebaddicts.com",
			allowedOrigins: []string{"https://admin.thewebaddicts.com"},
			want:           true,
		},
		{
			name:        "admin origin requires configuration",
			requestHost: "91-98-82-198-heya-service.twalab.cloud",
			origin:      "https://admin.thewebaddicts.com",
			want:        false,
		},
		{
			name:           "configured origin is scheme exact",
			requestHost:    "91-98-82-198-heya-service.twalab.cloud",
			origin:         "http://admin.thewebaddicts.com",
			allowedOrigins: []string{"https://admin.thewebaddicts.com"},
			want:           false,
		},
		{
			name:           "unrelated origin",
			requestHost:    "91-98-82-198-heya-service.twalab.cloud",
			origin:         "https://example.com",
			allowedOrigins: []string{"https://admin.thewebaddicts.com"},
			want:           false,
		},
		{
			name:           "origin with path is rejected",
			requestHost:    "91-98-82-198-heya-service.twalab.cloud",
			origin:         "https://admin.thewebaddicts.com/app",
			allowedOrigins: []string{"https://admin.thewebaddicts.com"},
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://"+tt.requestHost+"/dev/run", nil)
			req.Host = tt.requestHost
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}

			got := isAllowedWebSocketOrigin(req, tt.allowedOrigins)
			if got != tt.want {
				t.Fatalf("isAllowedWebSocketOrigin() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestDevProxyHostFromServerIP(t *testing.T) {
	got := devProxyHostFromServerIP("91.98.82.198")
	want := "91-98-82-198-heya-service.twalab.cloud"
	if got != want {
		t.Fatalf("devProxyHostFromServerIP() = %q, want %q", got, want)
	}
}

func TestDevProxyAppPathFromStoreAndInstallation(t *testing.T) {
	values := url.Values{
		"storeUUID":      []string{"57726969-9e2e-11ed-9f8e-42010a960004"},
		"installationID": []string{"z-6a1ef6c3dcca6"},
	}

	got, query, err := devProxyAppPathFromQuery(values)
	if err != nil {
		t.Fatalf("devProxyAppPathFromQuery() error = %v", err)
	}
	want := "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6"
	if got != want {
		t.Fatalf("devProxyAppPathFromQuery() = %q, want %q", got, want)
	}
	if query != "" {
		t.Fatalf("query = %q, want empty", query)
	}
}

func TestDevProxyAppPathFromPreviewURL(t *testing.T) {
	values := url.Values{
		"previewUrl": []string{"https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6?preview=true"},
	}

	got, query, err := devProxyAppPathFromQuery(values)
	if err != nil {
		t.Fatalf("devProxyAppPathFromQuery() error = %v", err)
	}
	want := "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6"
	if got != want {
		t.Fatalf("devProxyAppPathFromQuery() = %q, want %q", got, want)
	}
	if query != "preview=true" {
		t.Fatalf("query = %q, want preview=true", query)
	}
}

func TestDevProxyAppPathFromPageURL(t *testing.T) {
	values := url.Values{
		"pageUrl": []string{"https://admin.thewebaddicts.com/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6?path=%2F"},
	}

	got, query, err := devProxyAppPathFromQuery(values)
	if err != nil {
		t.Fatalf("devProxyAppPathFromQuery() error = %v", err)
	}
	want := "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6"
	if got != want {
		t.Fatalf("devProxyAppPathFromQuery() = %q, want %q", got, want)
	}
	if query != "path=%2F" {
		t.Fatalf("query = %q, want path=%%2F", query)
	}
}

func TestDevRunAppURLDoesNotDuplicateThemeBasePath(t *testing.T) {
	rootURL := "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/"
	basePath := "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/"

	tests := []struct {
		name    string
		appPath string
		want    string
	}{
		{
			name:    "theme homepage without trailing slash",
			appPath: "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6",
			want:    rootURL,
		},
		{
			name:    "theme homepage with trailing slash",
			appPath: "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/",
			want:    rootURL,
		},
		{
			name:    "nested theme route",
			appPath: "/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/products",
			want:    rootURL + "products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := devRunAppURL(rootURL, basePath, tt.appPath)
			if got != tt.want {
				t.Fatalf("devRunAppURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDevRunAppURLPreservesNonBaseRelativeAppPath(t *testing.T) {
	rootURL := "https://91-98-82-198-heya-service.twalab.cloud/dev/proxy/energy-user/"
	got := devRunAppURL(rootURL, "/dev/proxy/energy-user/", "/settings")
	want := "https://91-98-82-198-heya-service.twalab.cloud/dev/proxy/energy-user/settings"
	if got != want {
		t.Fatalf("devRunAppURL() = %q, want %q", got, want)
	}
}

func dialWebSocket(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	return dialWebSocketWithHeaders(t, url, nil)
}

func dialWebSocketWithHeaders(t *testing.T, url string, requestHeader http.Header) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial(url, requestHeader)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	return conn
}

func readWebSocketMap(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	var message map[string]any
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
	return message
}

func readWebSocketMessage(t *testing.T, conn *websocket.Conn) struct {
	DevServerURL string `json:"devServerURL"`
	DevProxyURL  string `json:"devProxyURL"`
	Connections  int    `json:"connections"`
	Run          struct {
		DevServerURL      string `json:"devServerURL"`
		DevServerBasePath string `json:"devServerBasePath"`
	} `json:"run"`
} {
	t.Helper()

	var message struct {
		DevServerURL string `json:"devServerURL"`
		DevProxyURL  string `json:"devProxyURL"`
		Connections  int    `json:"connections"`
		Run          struct {
			DevServerURL      string `json:"devServerURL"`
			DevServerBasePath string `json:"devServerBasePath"`
		} `json:"run"`
	}
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
	return message
}

func serverPort(t *testing.T, rawURL string) int {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	_, rawPort, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return port
}

func waitForCounts(t *testing.T, runner *fakeRunner, wantRunCount, wantStopCount int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runCount, stopCount := runner.counts()
		if runCount == wantRunCount && stopCount == wantStopCount {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	runCount, stopCount := runner.counts()
	t.Fatalf("counts = run %d stop %d, want run %d stop %d", runCount, stopCount, wantRunCount, wantStopCount)
}
