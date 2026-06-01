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
