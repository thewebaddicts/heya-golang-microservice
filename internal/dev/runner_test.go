package dev

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestShellDevCommandUsesNPMAndPort(t *testing.T) {
	got := shellDevCommand("npm", 3002)
	want := "'npm' run dev -- --port 3002"
	if got != want {
		t.Fatalf("shellDevCommand() = %q, want %q", got, want)
	}
}

func TestShellDevCommandQuotesNPMPath(t *testing.T) {
	got := shellDevCommand("/path with spaces/npm", 3002)
	want := "'/path with spaces/npm' run dev -- --port 3002"
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

func TestLocalLogFileIncludesProjectAndPort(t *testing.T) {
	got := localLogFile("/tmp/heya", "/srv/apps/my solid app", 3002, time.Date(2026, 5, 22, 10, 30, 0, 0, time.UTC))
	want := filepath.Join("/tmp/heya", "my-solid-app-3002-20260522T103000Z.log")
	if got != want {
		t.Fatalf("localLogFile() = %q, want %q", got, want)
	}
}
