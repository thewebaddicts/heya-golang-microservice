package dev

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"heya-golang-microservice/internal/config"
)

type Manager struct {
	cfg      config.Config
	runner   Runner
	logger   *slog.Logger
	mu       sync.Mutex
	sessions map[string]*managedSession
}

type managedSession struct {
	result      RunResult
	connections int
	starting    bool
	stopping    bool
	idleTimer   *time.Timer
	ready       chan struct{}
}

type Lease struct {
	manager *Manager
	key     string
	Result  RunResult
	Count   int

	once sync.Once
	err  error
}

func NewManager(cfg config.Config, runner Runner, logger *slog.Logger) *Manager {
	return &Manager{
		cfg:      cfg,
		runner:   runner,
		logger:   logger,
		sessions: make(map[string]*managedSession),
	}
}

func (m *Manager) Acquire(ctx context.Context, req RunRequest) (*Lease, error) {
	normalized, key, err := m.normalize(req)
	if err != nil {
		return nil, err
	}

	for {
		m.mu.Lock()
		if session, ok := m.sessions[key]; ok {
			if session.starting || session.stopping {
				ready := session.ready
				m.mu.Unlock()

				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ready:
					continue
				}
			}

			if session.idleTimer != nil {
				session.idleTimer.Stop()
				session.idleTimer = nil
			}
			session.connections++
			count := session.connections
			result := session.result
			m.mu.Unlock()

			m.logger.Info("reusing SolidJS dev server",
				"projectPath", result.ProjectPath,
				"port", result.Port,
				"connections", count,
			)

			return &Lease{
				manager: m,
				key:     key,
				Result:  result,
				Count:   count,
			}, nil
		}

		session := &managedSession{
			connections: 1,
			starting:    true,
			ready:       make(chan struct{}),
		}
		m.sessions[key] = session
		m.mu.Unlock()

		result, err := m.runner.Run(ctx, normalized)

		m.mu.Lock()
		session.starting = false
		if err != nil {
			delete(m.sessions, key)
			close(session.ready)
			m.mu.Unlock()
			return nil, err
		}

		session.result = result
		close(session.ready)
		m.mu.Unlock()

		return &Lease{
			manager: m,
			key:     key,
			Result:  result,
			Count:   1,
		}, nil
	}
}

func (l *Lease) Release(ctx context.Context) error {
	l.once.Do(func() {
		l.err = l.manager.release(ctx, l.key)
	})
	return l.err
}

func (m *Manager) release(ctx context.Context, key string) error {
	m.mu.Lock()
	session, ok := m.sessions[key]
	if !ok {
		m.mu.Unlock()
		return nil
	}

	if session.connections > 1 {
		session.connections--
		count := session.connections
		result := session.result
		m.mu.Unlock()

		m.logger.Info("released SolidJS dev server connection",
			"projectPath", result.ProjectPath,
			"port", result.Port,
			"connections", count,
		)
		return nil
	}

	session.connections = 0
	count := session.connections
	result := session.result
	idleTimeout := m.devIdleTimeout()
	if idleTimeout > 0 {
		if session.idleTimer != nil {
			session.idleTimer.Stop()
		}
		session.idleTimer = time.AfterFunc(idleTimeout, func() {
			m.stopIdleSession(key)
		})
		m.mu.Unlock()

		m.logger.Info("released SolidJS dev server connection; scheduled idle stop",
			"projectPath", result.ProjectPath,
			"port", result.Port,
			"connections", count,
			"idleTimeout", idleTimeout,
		)
		return nil
	}
	m.mu.Unlock()

	return m.stopSession(ctx, key)
}

func (m *Manager) stopIdleSession(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), m.processStopTimeout())
	defer cancel()

	if err := m.stopSession(ctx, key); err != nil {
		m.logger.Error("failed to stop idle SolidJS dev server", "error", err)
	}
}

func (m *Manager) stopSession(ctx context.Context, key string) error {
	m.mu.Lock()
	session, ok := m.sessions[key]
	if !ok {
		m.mu.Unlock()
		return nil
	}
	if session.connections > 0 {
		m.mu.Unlock()
		return nil
	}
	if session.stopping {
		m.mu.Unlock()
		return nil
	}

	session.stopping = true
	session.ready = make(chan struct{})
	result := session.result
	if session.idleTimer != nil {
		session.idleTimer.Stop()
		session.idleTimer = nil
	}
	m.mu.Unlock()

	err := m.runner.Stop(ctx, result)

	m.mu.Lock()
	delete(m.sessions, key)
	close(session.ready)
	m.mu.Unlock()

	if err != nil {
		m.logger.Error("failed to stop SolidJS dev server",
			"error", err,
			"projectPath", result.ProjectPath,
			"port", result.Port,
			"pid", result.PID,
		)
		return err
	}

	m.logger.Info("stopped SolidJS dev server",
		"projectPath", result.ProjectPath,
		"port", result.Port,
		"pid", result.PID,
	)
	return nil
}

func (m *Manager) devIdleTimeout() time.Duration {
	if m.cfg.DevIdleTimeout > 0 {
		return m.cfg.DevIdleTimeout
	}
	return 30 * time.Second
}

func (m *Manager) processStopTimeout() time.Duration {
	if m.cfg.ProcessStopTimeout > 0 {
		return m.cfg.ProcessStopTimeout
	}
	return 15 * time.Second
}

func (m *Manager) normalize(req RunRequest) (RunRequest, string, error) {
	port := req.Port
	if port == 0 {
		port = m.cfg.DefaultDevPort
	}
	if port < 1 || port > 65535 {
		return RunRequest{}, "", config.ValidationError("port must be between 1 and 65535")
	}

	projectDir, err := m.cfg.ResolveProjectDir(req.ProjectPath)
	if err != nil {
		return RunRequest{}, "", err
	}

	normalized := RunRequest{
		ProjectPath:       projectDir,
		Port:              port,
		DevServerHost:     req.DevServerHost,
		DevServerBasePath: req.DevServerBasePath,
	}
	key := fmt.Sprintf("%s:%d", projectDir, port)
	return normalized, key, nil
}
