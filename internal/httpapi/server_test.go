package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
		ProjectPath:  projectPath,
		Port:         port,
		DevServerURL: fmt.Sprintf("http://%s:%d", host, port),
		PID:          "12345",
		LogFile:      "/tmp/heya.log",
		Command:      fmt.Sprintf("npm run dev -- --port %d", port),
		Target:       "127.0.0.1:22",
		StartedAt:    time.Now().UTC(),
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

	if message.DevServerURL != "http://91.98.82.198:12017" {
		t.Fatalf("devServerURL = %q, want %q", message.DevServerURL, "http://91.98.82.198:12017")
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

func dialWebSocket(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
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
	Connections  int    `json:"connections"`
} {
	t.Helper()

	var message struct {
		DevServerURL string `json:"devServerURL"`
		Connections  int    `json:"connections"`
	}
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatalf("ReadJSON() error = %v", err)
	}
	return message
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
