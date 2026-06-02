package dev

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"heya-golang-microservice/internal/config"
)

func TestShellDevCommandUsesNPMAndPort(t *testing.T) {
	got := shellDevCommand("npm", "0.0.0.0", "", "", 3002)
	want := "'npm' run dev -- --host '0.0.0.0' --port 3002 --strictPort"
	if got != want {
		t.Fatalf("shellDevCommand() = %q, want %q", got, want)
	}
}

func TestShellDevCommandQuotesNPMPath(t *testing.T) {
	got := shellDevCommand("/path with spaces/npm", "0.0.0.0", "", "", 3002)
	want := "'/path with spaces/npm' run dev -- --host '0.0.0.0' --port 3002 --strictPort"
	if got != want {
		t.Fatalf("shellDevCommand() = %q, want %q", got, want)
	}
}

func TestShellDevCommandDefaultsBindHost(t *testing.T) {
	got := shellDevCommand("npm", "", "", "", 3002)
	want := "'npm' run dev -- --host '0.0.0.0' --port 3002 --strictPort"
	if got != want {
		t.Fatalf("shellDevCommand() = %q, want %q", got, want)
	}
}

func TestShellDevCommandIncludesBasePath(t *testing.T) {
	got := shellDevCommand("npm", "0.0.0.0", "/dev/proxy/energy-user/", "", 3002)
	want := "'npm' run dev -- --host '0.0.0.0' --port 3002 --strictPort --base '/dev/proxy/energy-user/'"
	if got != want {
		t.Fatalf("shellDevCommand() = %q, want %q", got, want)
	}
}

func TestShellDevCommandIncludesConfigFile(t *testing.T) {
	got := shellDevCommand("npm", "0.0.0.0", "/themes/store/install/", "/tmp/vite proxy.mjs", 3002)
	want := "'npm' run dev -- --host '0.0.0.0' --port 3002 --strictPort --base '/themes/store/install/' --config '/tmp/vite proxy.mjs'"
	if got != want {
		t.Fatalf("shellDevCommand() = %q, want %q", got, want)
	}
}

func TestShellArgsUseLoginShellForZsh(t *testing.T) {
	got := shellArgs("/bin/zsh", "npm run dev")
	if len(got) != 2 || got[0] != "-lc" || got[1] != "npm run dev" {
		t.Fatalf("shellArgs() = %#v", got)
	}
}

func TestPrependPathUpdatesExistingPath(t *testing.T) {
	got := prependPath([]string{"PATH=/usr/bin", "HOME=/tmp"}, "/opt/homebrew/bin")
	if got[0] != "PATH=/opt/homebrew/bin"+string(os.PathListSeparator)+"/usr/bin" {
		t.Fatalf("PATH entry = %q", got[0])
	}
}

func TestDevServerEnvironmentAddsViteAllowedHost(t *testing.T) {
	got := devServerEnvironment([]string{"PATH=/usr/bin"}, "91-98-82-198-heya-service.twalab.cloud")
	want := "__VITE_ADDITIONAL_SERVER_ALLOWED_HOSTS=91-98-82-198-heya-service.twalab.cloud"
	if !containsEnv(got, want) {
		t.Fatalf("environment does not contain %q: %#v", want, got)
	}
}

func TestViteProxyConfigSourceMergesAllowedHostAndHMR(t *testing.T) {
	got := viteProxyConfigSource("/srv/app/vite.config.ts", "91-98-82-198-heya-service.twalab.cloud", "/themes/store/install/")
	for _, want := range []string{
		`import originalConfigModule from "file:///srv/app/vite.config.ts"`,
		`const proxyAllowedHost = "91-98-82-198-heya-service.twalab.cloud"`,
		`const proxyHMRPath = "/themes/store/install/__vite_hmr"`,
		`allowedHosts = Array.from(new Set([...hostList, proxyAllowedHost]))`,
		`protocol: "wss"`,
		`clientPort: 443`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("viteProxyConfigSource() missing %q in:\n%s", want, got)
		}
	}
}

func containsEnv(env []string, want string) bool {
	for _, entry := range env {
		if entry == want {
			return true
		}
	}
	return false
}

func TestParseProcessIDRejectsInvalidPID(t *testing.T) {
	_, err := parseProcessID("123; rm -rf /")
	if err == nil {
		t.Fatal("parseProcessID() error = nil, want error")
	}
}

func TestParseProcessIDRejectsNonPositivePID(t *testing.T) {
	_, err := parseProcessID("0")
	if err != nil {
		return
	}
	t.Fatal("parseProcessID() error = nil, want error")
}

func TestParseProcessID(t *testing.T) {
	got, err := parseProcessID("12345")
	if err != nil {
		t.Fatalf("parseProcessID() error = %v", err)
	}
	if got != 12345 {
		t.Fatalf("parseProcessID() = %d, want %d", got, 12345)
	}
}

func TestDevServerURLUsesPublicHostOverride(t *testing.T) {
	runner := NewLocalRunner(config.Config{
		DevServerScheme: "http",
		DevServerHost:   "localhost",
		DevReadyHost:    "localhost",
	}, nil)

	got := runner.devServerURL(12036, "91.98.82.198")
	want := "http://91.98.82.198:12036"
	if got != want {
		t.Fatalf("devServerURL() = %q, want %q", got, want)
	}
}

func TestDevReadyURLDefaultsToLocalhost(t *testing.T) {
	runner := NewLocalRunner(config.Config{
		DevServerScheme: "http",
		DevServerHost:   "91.98.82.198",
	}, nil)

	got := runner.devReadyURL(12036)
	want := "http://localhost:12036"
	if got != want {
		t.Fatalf("devReadyURL() = %q, want %q", got, want)
	}
}

func TestDevReadyURLUsesConfiguredHost(t *testing.T) {
	runner := NewLocalRunner(config.Config{
		DevReadyHost: "127.0.0.1",
	}, nil)

	got := runner.devReadyURL(12036)
	want := "http://127.0.0.1:12036"
	if got != want {
		t.Fatalf("devReadyURL() = %q, want %q", got, want)
	}
}

func TestDevServerBindHostDefaultsToAllInterfaces(t *testing.T) {
	runner := NewLocalRunner(config.Config{}, nil)

	got := runner.devServerBindHost()
	want := "0.0.0.0"
	if got != want {
		t.Fatalf("devServerBindHost() = %q, want %q", got, want)
	}
}

func TestDevBindURLUsesConfiguredBindHost(t *testing.T) {
	runner := NewLocalRunner(config.Config{}, nil)

	got := runner.devBindURL(12036, "0.0.0.0")
	want := "http://0.0.0.0:12036"
	if got != want {
		t.Fatalf("devBindURL() = %q, want %q", got, want)
	}
}

func TestDevLocalURLUsesLocalhost(t *testing.T) {
	runner := NewLocalRunner(config.Config{}, nil)

	got := runner.devLocalURL(12036)
	want := "http://localhost:12036"
	if got != want {
		t.Fatalf("devLocalURL() = %q, want %q", got, want)
	}
}

func TestLocalLogFileIncludesProjectAndPort(t *testing.T) {
	got := localLogFile("/tmp/heya", "/srv/apps/my solid app", 3002, time.Date(2026, 5, 22, 10, 30, 0, 0, time.UTC))
	want := filepath.Join("/tmp/heya", "my-solid-app-3002-20260522T103000Z.log")
	if got != want {
		t.Fatalf("localLogFile() = %q, want %q", got, want)
	}
}
