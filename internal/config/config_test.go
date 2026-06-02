package config

import (
	"path/filepath"
	"testing"
)

func TestResolveProjectDirUsesDefaultProjectDir(t *testing.T) {
	base := t.TempDir()
	defaultProject := filepath.Join(base, "solid-app")
	cfg := Config{ProjectBaseDir: base, DefaultProjectDir: defaultProject}

	got, err := cfg.ResolveProjectDir("")
	if err != nil {
		t.Fatalf("ResolveProjectDir() error = %v", err)
	}
	if got != defaultProject {
		t.Fatalf("ResolveProjectDir() = %q, want %q", got, defaultProject)
	}
}

func TestResolveProjectDirRejectsPathsOutsideBase(t *testing.T) {
	base := t.TempDir()
	cfg := Config{ProjectBaseDir: base}

	_, err := cfg.ResolveProjectDir("../outside")
	if err == nil {
		t.Fatal("ResolveProjectDir() error = nil, want error")
	}
}

func TestEnvOriginListUsesFallback(t *testing.T) {
	t.Setenv("HEYA_WEBSOCKET_ALLOWED_ORIGINS", "")

	got, err := envOriginList("HEYA_WEBSOCKET_ALLOWED_ORIGINS", []string{"https://admin.thewebaddicts.com"})
	if err != nil {
		t.Fatalf("envOriginList() error = %v", err)
	}
	if len(got) != 1 || got[0] != "https://admin.thewebaddicts.com" {
		t.Fatalf("envOriginList() = %#v, want default admin origin", got)
	}
}

func TestEnvOriginListParsesConfiguredOrigins(t *testing.T) {
	t.Setenv("HEYA_WEBSOCKET_ALLOWED_ORIGINS", "https://admin.thewebaddicts.com, http://localhost:5173/")

	got, err := envOriginList("HEYA_WEBSOCKET_ALLOWED_ORIGINS", nil)
	if err != nil {
		t.Fatalf("envOriginList() error = %v", err)
	}
	want := []string{"https://admin.thewebaddicts.com", "http://localhost:5173"}
	if len(got) != len(want) {
		t.Fatalf("envOriginList() = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("envOriginList()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestEnvOriginListRejectsInvalidOrigin(t *testing.T) {
	t.Setenv("HEYA_WEBSOCKET_ALLOWED_ORIGINS", "https://admin.thewebaddicts.com/app")

	_, err := envOriginList("HEYA_WEBSOCKET_ALLOWED_ORIGINS", nil)
	if err == nil {
		t.Fatal("envOriginList() error = nil, want error")
	}
}
