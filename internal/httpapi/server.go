package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"heya-golang-microservice/internal/account"
	"heya-golang-microservice/internal/buildrun"
	"heya-golang-microservice/internal/config"
	"heya-golang-microservice/internal/dev"
)

type Server struct {
	cfg          config.Config
	manager      *dev.Manager
	buildManager *buildrun.Manager
	accounts     *account.Resolver
	logger       *slog.Logger
}

func NewServer(cfg config.Config, runner dev.Runner, logger *slog.Logger) *Server {
	return &Server{
		cfg:          cfg,
		manager:      dev.NewManager(cfg, runner, logger),
		buildManager: buildrun.NewManager(cfg, buildrun.NewRunner(cfg), logger),
		accounts:     account.NewResolver(cfg, logger),
		logger:       logger,
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /dev/run", s.handleDevRunWebSocket)
	mux.HandleFunc("GET /build/run", s.handleBuildRunWebSocket)
	mux.HandleFunc("/dev/proxy", s.handleDevProxy)
	mux.HandleFunc("/dev/proxy/", s.handleDevProxy)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleDevRunWebSocket(w http.ResponseWriter, r *http.Request) {
	if !websocket.IsWebSocketUpgrade(r) {
		writeError(w, http.StatusUpgradeRequired, "websocket upgrade required")
		return
	}
	if !s.isAllowedWebSocketOrigin(r) {
		writeError(w, http.StatusForbidden, "websocket origin is not allowed")
		return
	}

	req, err := s.runRequestFromQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	projectUser := strings.TrimSpace(r.URL.Query().Get("projectUser"))
	if projectUser != "" {
		if err := s.resolveDevProjectUser(r.Context(), projectUser, &req); err != nil {
			s.logger.Error("failed to resolve dev project user", "projectUser", projectUser, "error", err)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.DevServerBasePath = devProxyBasePath(projectUser)
		s.logger.Info("resolved dev run project user",
			"projectUser", projectUser,
			"projectPath", req.ProjectPath,
			"port", req.Port,
			"basePath", req.DevServerBasePath,
		)
	}

	s.logger.Info("received dev run websocket request",
		"queryProjectPath", req.ProjectPath,
		"defaultProjectDir", s.cfg.DefaultProjectDir,
		"port", req.Port,
		"projectUser", projectUser,
	)

	upgrader := s.upgrader()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	lease, err := s.manager.Acquire(context.Background(), req)
	if err != nil {
		s.logger.Error("failed to start dev server", "error", err)
		_ = conn.WriteJSON(map[string]string{
			"type":   "error",
			"status": "failed",
			"error":  err.Error(),
		})
		return
	}

	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), s.processStopTimeout())
		defer cancel()
		if err := lease.Release(releaseCtx); err != nil {
			s.logger.Error("failed to release dev server websocket lease", "error", err)
		}
	}()

	payload := map[string]any{
		"type":         "dev_server",
		"status":       "running",
		"projectUser":  projectUser,
		"devServerURL": lease.Result.DevServerURL,
		"connections":  lease.Count,
		"run":          lease.Result,
	}
	if projectUser != "" {
		payload["devProxyURL"] = s.absoluteDevProxyURL(r, projectUser)
	}
	if err := conn.WriteJSON(payload); err != nil {
		return
	}

	s.waitForDisconnect(conn)
}

func (s *Server) runRequestFromQuery(values url.Values) (dev.RunRequest, error) {
	var port int
	rawPort := strings.TrimSpace(values.Get("port"))
	if rawPort != "" {
		parsed, err := strconv.Atoi(rawPort)
		if err != nil {
			return dev.RunRequest{}, config.ValidationError("port must be an integer")
		}
		port = parsed
	}

	return dev.RunRequest{
		ProjectPath: strings.TrimSpace(values.Get("projectPath")),
		Port:        port,
	}, nil
}

func queryBool(values url.Values, key string) bool {
	value := strings.ToLower(strings.TrimSpace(values.Get(key)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (s *Server) handleDevProxy(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) && !s.isAllowedWebSocketOrigin(r) {
		writeError(w, http.StatusForbidden, "websocket origin is not allowed")
		return
	}

	req, projectUser, upstreamPath, upstreamQuery, redirectURL, err := s.devProxyRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if redirectURL != "" {
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return
	}

	s.logger.Info("received dev proxy request",
		"projectUser", projectUser,
		"projectPath", req.ProjectPath,
		"port", req.Port,
		"basePath", req.DevServerBasePath,
		"upstreamPath", upstreamPath,
	)

	lease, err := s.manager.Acquire(r.Context(), req)
	if err != nil {
		s.logger.Error("failed to start dev server for proxy", "projectUser", projectUser, "error", err)
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), s.processStopTimeout())
		defer cancel()
		if err := lease.Release(releaseCtx); err != nil {
			s.logger.Error("failed to release dev proxy lease", "projectUser", projectUser, "error", err)
		}
	}()

	target := &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(s.devProxyTargetHost(), strconv.Itoa(lease.Result.Port)),
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(proxyReq *http.Request) {
		proxyReq.URL.Scheme = target.Scheme
		proxyReq.URL.Host = target.Host
		proxyReq.URL.Path = upstreamPath
		proxyReq.URL.RawPath = ""
		proxyReq.URL.RawQuery = upstreamQuery
		proxyReq.Host = target.Host
		proxyReq.Header.Set("X-Forwarded-Host", r.Host)
		proxyReq.Header.Set("X-Forwarded-Proto", requestScheme(r))
		if req.DevServerBasePath != "" {
			proxyReq.Header.Set("X-Forwarded-Prefix", strings.TrimSuffix(req.DevServerBasePath, "/"))
		}
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, proxyReq *http.Request, proxyErr error) {
		s.logger.Error("dev proxy request failed",
			"projectUser", projectUser,
			"target", target.String(),
			"path", upstreamPath,
			"error", proxyErr,
		)
		writeError(rw, http.StatusBadGateway, "dev proxy request failed")
	}
	proxy.ServeHTTP(w, r)
}

func (s *Server) devProxyRequest(r *http.Request) (dev.RunRequest, string, string, string, string, error) {
	req, err := s.runRequestFromQuery(r.URL.Query())
	if err != nil {
		return dev.RunRequest{}, "", "", "", "", err
	}

	projectUser, upstreamPath, redirectURL := devProxyPathParts(r)
	if projectUser == "" {
		projectUser = strings.TrimSpace(r.URL.Query().Get("projectUser"))
	}
	if upstreamPath == "" {
		upstreamPath = "/"
	}

	if projectUser != "" {
		if err := s.resolveDevProjectUser(r.Context(), projectUser, &req); err != nil {
			return dev.RunRequest{}, "", "", "", "", err
		}
		req.DevServerBasePath = devProxyBasePath(projectUser)
	} else if strings.TrimSpace(req.ProjectPath) == "" {
		return dev.RunRequest{}, "", "", "", "", config.ValidationError("projectUser is required for dev proxy")
	}

	upstreamQuery := cleanDevProxyQuery(r.URL.Query()).Encode()
	return req, projectUser, upstreamPath, upstreamQuery, redirectURL, nil
}

func devProxyPathParts(r *http.Request) (string, string, string) {
	if r.URL.Path == "/dev/proxy" {
		return "", "/", ""
	}

	pathPart := strings.TrimPrefix(r.URL.Path, "/dev/proxy/")
	if pathPart == "" {
		return "", "/", ""
	}

	separator := strings.Index(pathPart, "/")
	if separator == -1 {
		redirect := *r.URL
		redirect.Path = r.URL.Path + "/"
		return pathPart, "/", redirect.String()
	}

	projectUser := pathPart[:separator]
	upstreamPath := pathPart[separator:]
	if upstreamPath == "" {
		upstreamPath = "/"
	}
	return projectUser, upstreamPath, ""
}

func cleanDevProxyQuery(values url.Values) url.Values {
	cleaned := make(url.Values, len(values))
	for key, value := range values {
		switch key {
		case "projectUser", "projectPath", "port":
			continue
		default:
			cleaned[key] = append([]string(nil), value...)
		}
	}
	return cleaned
}

func (s *Server) resolveDevProjectUser(ctx context.Context, projectUser string, req *dev.RunRequest) error {
	info, err := s.accounts.Resolve(ctx, projectUser)
	if err != nil {
		return err
	}
	req.ProjectPath = info.WorkingDirectoryHeya
	req.Port = info.DevPort
	req.DevServerHost = info.ServerIP
	s.logger.Info("resolved dev project user",
		"projectUser", projectUser,
		"accountUsername", info.AccountUsername,
		"serverIP", info.ServerIP,
		"projectPath", req.ProjectPath,
		"port", req.Port,
	)
	return nil
}

func devProxyBasePath(projectUser string) string {
	return "/dev/proxy/" + url.PathEscape(projectUser) + "/"
}

func (s *Server) absoluteDevProxyURL(r *http.Request, projectUser string) string {
	return requestScheme(r) + "://" + r.Host + devProxyBasePath(projectUser)
}

func requestScheme(r *http.Request) string {
	forwardedProto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if forwardedProto != "" {
		if comma := strings.Index(forwardedProto, ","); comma >= 0 {
			forwardedProto = strings.TrimSpace(forwardedProto[:comma])
		}
		return forwardedProto
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func (s *Server) devProxyTargetHost() string {
	host := strings.TrimSpace(s.cfg.DevReadyHost)
	switch host {
	case "", "0.0.0.0":
		return "localhost"
	default:
		return host
	}
}

func (s *Server) handleBuildRunWebSocket(w http.ResponseWriter, r *http.Request) {
	if !websocket.IsWebSocketUpgrade(r) {
		writeError(w, http.StatusUpgradeRequired, "websocket upgrade required")
		return
	}
	if !s.isAllowedWebSocketOrigin(r) {
		writeError(w, http.StatusForbidden, "websocket origin is not allowed")
		return
	}

	projectPath := strings.TrimSpace(r.URL.Query().Get("projectPath"))
	projectUser := strings.TrimSpace(r.URL.Query().Get("projectUser"))
	mode := strings.TrimSpace(r.URL.Query().Get("mode"))
	watchOnly := queryBool(r.URL.Query(), "watch")
	if projectUser != "" {
		info, err := s.accounts.Resolve(r.Context(), projectUser)
		if err != nil {
			s.logger.Error("failed to resolve build project user", "projectUser", projectUser, "error", err)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		projectPath = info.WorkingDirectoryHeya
		s.logger.Info("resolved build run project user",
			"projectUser", projectUser,
			"accountUsername", info.AccountUsername,
			"serverIP", info.ServerIP,
			"projectPath", projectPath,
			"portDevLive", info.DevPort,
		)
	}
	s.logger.Info("received build run websocket request",
		"queryProjectPath", projectPath,
		"defaultProjectDir", s.cfg.DefaultProjectDir,
		"mode", mode,
		"watchOnly", watchOnly,
		"projectUser", projectUser,
	)

	upgrader := s.upgrader()
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	subscription, err := s.buildManager.Subscribe(buildrun.Request{
		ProjectPath: projectPath,
		Mode:        mode,
	}, buildrun.SubscribeOptions{Start: !watchOnly})
	if err != nil {
		s.logger.Error("failed to subscribe to build", "error", err)
		_ = conn.WriteJSON(map[string]string{
			"type":   "error",
			"status": "failed",
			"error":  err.Error(),
		})
		return
	}
	defer subscription.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case event, ok := <-subscription.Events:
			if !ok {
				return
			}
			if err := conn.WriteJSON(event); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (s *Server) upgrader() websocket.Upgrader {
	return websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return s.isAllowedWebSocketOrigin(r)
		},
	}
}

func (s *Server) isAllowedWebSocketOrigin(r *http.Request) bool {
	return isAllowedWebSocketOrigin(r, s.cfg.WebSocketAllowedOrigins)
}

func (s *Server) processStopTimeout() time.Duration {
	if s.cfg.ProcessStopTimeout > 0 {
		return s.cfg.ProcessStopTimeout
	}
	return 15 * time.Second
}

func (s *Server) waitForDisconnect(conn *websocket.Conn) {
	const (
		pongWait   = 60 * time.Second
		pingPeriod = 25 * time.Second
	)

	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				deadline := time.Now().Add(10 * time.Second)
				if err := conn.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()

	for {
		_, reader, err := conn.NextReader()
		if err != nil {
			close(done)
			return
		}
		_, _ = io.Copy(io.Discard, reader)
	}
}

func isAllowedWebSocketOrigin(r *http.Request, allowedOrigins []string) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}

	normalizedOrigin, originURL, ok := normalizeWebSocketOrigin(origin)
	if !ok {
		return false
	}

	originHost := hostname(originURL.Host)
	requestHost := hostname(r.Host)
	if originHost == "" || requestHost == "" {
		return false
	}

	if strings.EqualFold(originHost, requestHost) {
		return true
	}

	for _, allowedOrigin := range allowedOrigins {
		normalizedAllowedOrigin, _, ok := normalizeWebSocketOrigin(allowedOrigin)
		if ok && normalizedAllowedOrigin == normalizedOrigin {
			return true
		}
	}

	switch strings.ToLower(originHost) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func normalizeWebSocketOrigin(origin string) (string, *url.URL, bool) {
	originURL, err := url.Parse(strings.TrimSpace(origin))
	if err != nil {
		return "", nil, false
	}
	if originURL.Scheme == "" || originURL.Host == "" {
		return "", nil, false
	}
	scheme := strings.ToLower(originURL.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", nil, false
	}
	if originURL.User != nil || originURL.RawQuery != "" || originURL.Fragment != "" {
		return "", nil, false
	}
	if originURL.Path != "" && originURL.Path != "/" {
		return "", nil, false
	}

	return scheme + "://" + strings.ToLower(originURL.Host), originURL, true
}

func hostname(hostport string) string {
	host, _, err := net.SplitHostPort(hostport)
	if err == nil {
		return host
	}
	return strings.Trim(hostport, "[]")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}
