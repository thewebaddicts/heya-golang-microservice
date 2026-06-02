package account

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"heya-golang-microservice/internal/config"
)

func TestResolverCachesAccountInfo(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests > 1 {
			http.Error(w, "too many attempts", http.StatusTooManyRequests)
			return
		}
		writeAccountInfo(t, w, "energy-user", 12017)
	}))
	defer server.Close()

	resolver := NewResolver(config.Config{
		AccountInfoURL:      server.URL,
		AccountInfoToken:    "test-token",
		AccountInfoCacheTTL: 10 * time.Minute,
	}, nil)

	first, err := resolver.Resolve(context.Background(), "energy-user")
	if err != nil {
		t.Fatalf("first Resolve() error = %v", err)
	}
	second, err := resolver.Resolve(context.Background(), "energy-user")
	if err != nil {
		t.Fatalf("second Resolve() error = %v", err)
	}
	if first != second {
		t.Fatalf("second Resolve() = %#v, want cached first response %#v", second, first)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
}

func TestResolverCacheExpires(t *testing.T) {
	now := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		writeAccountInfo(t, w, "energy-user", 12016+requests)
	}))
	defer server.Close()

	resolver := NewResolver(config.Config{
		AccountInfoURL:      server.URL,
		AccountInfoToken:    "test-token",
		AccountInfoCacheTTL: 10 * time.Minute,
	}, nil)
	resolver.now = func() time.Time { return now }

	first, err := resolver.Resolve(context.Background(), "energy-user")
	if err != nil {
		t.Fatalf("first Resolve() error = %v", err)
	}
	now = now.Add(11 * time.Minute)
	second, err := resolver.Resolve(context.Background(), "energy-user")
	if err != nil {
		t.Fatalf("second Resolve() error = %v", err)
	}
	if first.DevPort == second.DevPort {
		t.Fatalf("second DevPort = %d, want refreshed value different from first", second.DevPort)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestResolverSharesConcurrentAccountInfoRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		once.Do(func() { close(started) })
		<-release
		writeAccountInfo(t, w, "energy-user", 12017)
	}))
	defer server.Close()

	resolver := NewResolver(config.Config{
		AccountInfoURL:      server.URL,
		AccountInfoToken:    "test-token",
		AccountInfoCacheTTL: 10 * time.Minute,
	}, nil)

	const clients = 8
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := resolver.Resolve(context.Background(), "energy-user")
			errs <- err
		}()
	}

	<-started
	close(release)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
}

func writeAccountInfo(t *testing.T, w http.ResponseWriter, username string, port int) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"account": map[string]any{
			"id":            257,
			"uuid":          "account-uuid",
			"username":      username,
			"label":         "Energy Bridge",
			"port_dev_live": port,
		},
		"server_ip":              "91.98.82.198",
		"working_directory":      "/home/" + username + "/public_html",
		"working_directory_heya": "/home/" + username + "/public_html/storage/app/frontend",
	}); err != nil {
		t.Fatalf("write response: %v", err)
	}
}
