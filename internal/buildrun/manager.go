package buildrun

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"heya-golang-microservice/internal/config"
)

type SubscribeOptions struct {
	Start bool
}

type Event struct {
	Type              string `json:"type"`
	Status            string `json:"status,omitempty"`
	Mode              string `json:"mode,omitempty"`
	SourceProjectPath string `json:"sourceProjectPath,omitempty"`
	BuildProjectPath  string `json:"buildProjectPath,omitempty"`
	Stream            string `json:"stream,omitempty"`
	Data              string `json:"data,omitempty"`
	Running           bool   `json:"running,omitempty"`
	Attached          bool   `json:"attached,omitempty"`
	ExitCode          *int   `json:"exitCode,omitempty"`
	Error             string `json:"error,omitempty"`
	Build             any    `json:"build,omitempty"`
}

type Subscription struct {
	Events  <-chan Event
	release func()
	once    sync.Once
}

func (s *Subscription) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		if s.release != nil {
			s.release()
		}
	})
}

type Manager struct {
	cfg              config.Config
	runner           *Runner
	logger           *slog.Logger
	mu               sync.Mutex
	jobs             map[string]*managedBuild
	nextSubscriberID int64
}

type managedBuild struct {
	key               string
	mode              string
	sourceProjectPath string
	buildProjectPath  string
	status            string
	started           *Started
	subscribers       map[int64]chan Event
}

func NewManager(cfg config.Config, runner *Runner, logger *slog.Logger) *Manager {
	if runner == nil {
		runner = NewRunner(cfg)
	}
	return &Manager{
		cfg:    cfg,
		runner: runner,
		logger: logger,
		jobs:   make(map[string]*managedBuild),
	}
}

func (m *Manager) Subscribe(req Request, opts SubscribeOptions) (*Subscription, error) {
	sourceDir, err := m.cfg.ResolveProjectDir(req.ProjectPath)
	if err != nil {
		return nil, err
	}
	if err := validateProjectDir(sourceDir); err != nil {
		return nil, err
	}

	mode, err := normalizeMode(req.Mode)
	if err != nil {
		return nil, err
	}

	key := buildKey(sourceDir, mode)
	m.mu.Lock()
	defer m.mu.Unlock()

	job := m.jobs[key]
	if job == nil {
		if !opts.Start {
			return idleSubscription(sourceDir, mode), nil
		}

		job = &managedBuild{
			key:               key,
			mode:              mode,
			sourceProjectPath: sourceDir,
			status:            "preparing",
			subscribers:       make(map[int64]chan Event),
		}
		m.jobs[key] = job
		go m.run(job)
	}

	id := m.nextSubscriberID
	m.nextSubscriberID++
	events := make(chan Event, 512)
	job.subscribers[id] = events
	sendEventLocked(events, job.snapshotEventLocked(true))

	return &Subscription{
		Events: events,
		release: func() {
			m.removeSubscriber(key, id)
		},
	}, nil
}

func (m *Manager) run(job *managedBuild) {
	req := Request{
		ProjectPath: job.sourceProjectPath,
		Mode:        job.mode,
	}
	complete, err := m.runner.Run(context.Background(), req, func(started Started) error {
		m.buildStarted(job.key, started)
		return nil
	}, func(output Output) error {
		m.buildOutput(job.key, output)
		return nil
	})

	if err != nil && complete.Status == "" {
		m.buildError(job.key, err)
		return
	}
	if err != nil && m.logger != nil {
		m.logger.Error("build completed with error", "error", err, "status", complete.Status, "exitCode", complete.ExitCode)
	}

	m.buildComplete(job.key, complete)
}

func (m *Manager) buildStarted(key string, started Started) {
	if m.logger != nil {
		m.logger.Info("build command started",
			"mode", started.Mode,
			"sourceProjectPath", started.SourceProjectPath,
			"buildProjectPath", started.BuildProjectPath,
			"command", started.Command,
			"pid", started.PID,
		)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	job := m.jobs[key]
	if job == nil {
		return
	}
	job.status = "building"
	job.buildProjectPath = started.BuildProjectPath
	startedCopy := started
	job.started = &startedCopy
	m.broadcastLocked(job, Event{
		Type:  "build_started",
		Build: started,
	})
}

func (m *Manager) buildOutput(key string, output Output) {
	if m.logger != nil {
		m.logger.Info("build output",
			"stream", output.Stream,
			"line", output.Data,
		)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	job := m.jobs[key]
	if job == nil {
		return
	}
	if status := statusFromOutput(output); status != "" {
		job.status = status
	}
	m.broadcastLocked(job, Event{
		Type:   "build_output",
		Stream: output.Stream,
		Data:   output.Data,
	})
}

func (m *Manager) buildComplete(key string, complete Complete) {
	if m.logger != nil {
		m.logger.Info("build completed",
			"status", complete.Status,
			"exitCode", complete.ExitCode,
			"sourceProjectPath", complete.SourceProjectPath,
			"buildProjectPath", complete.BuildProjectPath,
			"artifactPaths", complete.ArtifactPaths,
		)
	}

	exitCode := complete.ExitCode
	m.finish(key, Event{
		Type:     "build_complete",
		Status:   complete.Status,
		ExitCode: &exitCode,
		Build:    complete,
	})
}

func (m *Manager) buildError(key string, err error) {
	if m.logger != nil {
		m.logger.Error("failed to run build", "error", err)
	}
	m.finish(key, Event{
		Type:   "error",
		Status: "failed",
		Error:  err.Error(),
	})
}

func (m *Manager) finish(key string, event Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job := m.jobs[key]
	if job == nil {
		return
	}
	job.status = event.Status
	m.broadcastLocked(job, event)
	for id, events := range job.subscribers {
		delete(job.subscribers, id)
		close(events)
	}
	delete(m.jobs, key)
}

func (m *Manager) removeSubscriber(key string, id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job := m.jobs[key]
	if job == nil {
		return
	}
	events, ok := job.subscribers[id]
	if !ok {
		return
	}
	delete(job.subscribers, id)
	close(events)
}

func (m *Manager) broadcastLocked(job *managedBuild, event Event) {
	for _, events := range job.subscribers {
		sendEventLocked(events, event)
	}
}

func (j *managedBuild) snapshotEventLocked(attached bool) Event {
	return Event{
		Type:              "build_status",
		Status:            j.status,
		Mode:              j.mode,
		SourceProjectPath: j.sourceProjectPath,
		BuildProjectPath:  j.buildProjectPath,
		Running:           true,
		Attached:          attached,
	}
}

func idleSubscription(sourceDir string, mode string) *Subscription {
	events := make(chan Event, 1)
	events <- Event{
		Type:              "build_status",
		Status:            "idle",
		Mode:              mode,
		SourceProjectPath: sourceDir,
		Running:           false,
	}
	close(events)
	return &Subscription{
		Events:  events,
		release: func() {},
	}
}

func sendEventLocked(events chan Event, event Event) {
	select {
	case events <- event:
		return
	default:
	}

	if event.Type == "build_output" {
		return
	}

	for {
		select {
		case <-events:
		default:
			select {
			case events <- event:
			default:
			}
			return
		}
	}
}

func statusFromOutput(output Output) string {
	text := strings.ToLower(output.Stream + " " + output.Data)
	switch {
	case strings.Contains(text, "copying project"):
		return "preparing"
	case strings.Contains(text, "npm ci"), strings.Contains(text, "npm install"):
		return "installing"
	default:
		return ""
	}
}

func buildKey(sourceDir string, mode string) string {
	return sourceDir + "\x00" + mode
}
