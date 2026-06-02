package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"heya-golang-microservice/internal/account"
	"heya-golang-microservice/internal/buildrun"
	"heya-golang-microservice/internal/config"
	"heya-golang-microservice/internal/dev"
)

type Server struct {
	cfg              config.Config
	manager          *dev.Manager
	buildManager     *buildrun.Manager
	accounts         *account.Resolver
	logger           *slog.Logger
	themeProxyMu     sync.RWMutex
	themeProxyRoutes map[string]themeProxyRoute
}

type themeProxyRoute struct {
	ProjectUser string
	Request     dev.RunRequest
}

type devRunProxyURLs struct {
	AppURL   string
	RootURL  string
	BasePath string
	IsTheme  bool
}

var viteHMRPortPattern = regexp.MustCompile(`const hmrPort = \d+;`)

func NewServer(cfg config.Config, runner dev.Runner, logger *slog.Logger) *Server {
	return &Server{
		cfg:              cfg,
		manager:          dev.NewManager(cfg, runner, logger),
		buildManager:     buildrun.NewManager(cfg, buildrun.NewRunner(cfg), logger),
		accounts:         account.NewResolver(cfg, logger),
		logger:           logger,
		themeProxyRoutes: make(map[string]themeProxyRoute),
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /dev/run", s.handleDevRunWebSocket)
	mux.HandleFunc("GET /build/run", s.handleBuildRunWebSocket)
	mux.HandleFunc("/dev/proxy", s.handleDevProxy)
	mux.HandleFunc("/dev/proxy/", s.handleDevProxy)
	mux.HandleFunc("/_build/", s.handleThemeRootProxy)
	mux.HandleFunc("/api/theme-page/", s.handleThemeRootProxy)
	mux.HandleFunc("/api/theme-watch/", s.handleThemeRootProxy)
	mux.HandleFunc("/themes/", s.handleThemeProxy)
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
	var proxyURLs devRunProxyURLs
	if projectUser != "" {
		if err := s.resolveDevProjectUser(r.Context(), projectUser, &req); err != nil {
			s.logger.Error("failed to resolve dev project user", "projectUser", projectUser, "error", err)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		proxyURLs, err = s.devRunProxyURLs(r, projectUser, req.DevServerHost)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.DevServerBasePath = proxyURLs.BasePath
		req.DevServerPublicHost = hostFromAbsoluteURL(proxyURLs.RootURL)
		if proxyURLs.IsTheme {
			if err := s.ensureThemeProxyRouteAvailable(proxyURLs.BasePath, projectUser); err != nil {
				writeError(w, http.StatusConflict, err.Error())
				return
			}
		}
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

	if projectUser != "" && req.DevServerBasePath != "" && lease.Result.DevServerBasePath != req.DevServerBasePath {
		_ = conn.WriteJSON(map[string]string{
			"type":   "error",
			"status": "failed",
			"error":  "dev server is already running with base path " + lease.Result.DevServerBasePath + "; requested " + req.DevServerBasePath,
		})
		return
	}

	if projectUser != "" && req.DevServerPublicHost != "" && lease.Result.DevServerPublicHost != "" && lease.Result.DevServerPublicHost != req.DevServerPublicHost {
		_ = conn.WriteJSON(map[string]string{
			"type":   "error",
			"status": "failed",
			"error":  "dev server is already running with public host " + lease.Result.DevServerPublicHost + "; requested " + req.DevServerPublicHost,
		})
		return
	}

	if projectUser != "" && proxyURLs.IsTheme {
		if err := s.registerThemeProxyRoute(proxyURLs.BasePath, projectUser, req); err != nil {
			_ = conn.WriteJSON(map[string]string{
				"type":   "error",
				"status": "failed",
				"error":  err.Error(),
			})
			return
		}
	}

	payload := map[string]any{
		"type":         "dev_server",
		"status":       "running",
		"projectUser":  projectUser,
		"devServerURL": lease.Result.DevServerURL,
		"connections":  lease.Count,
		"run":          lease.Result,
	}
	if projectUser != "" {
		run := lease.Result
		run.DevServerURL = proxyURLs.AppURL
		payload["devServerURL"] = proxyURLs.AppURL
		payload["devProxyURL"] = proxyURLs.RootURL
		payload["run"] = run
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

	s.proxyDevServer(w, r, req, projectUser, upstreamPath, upstreamQuery)
}

func (s *Server) handleThemeProxy(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) && !s.isAllowedWebSocketOrigin(r) {
		writeError(w, http.StatusForbidden, "websocket origin is not allowed")
		return
	}

	route, basePath, ok := s.themeProxyRouteForPath(r.URL.EscapedPath())
	if !ok {
		writeError(w, http.StatusNotFound, "theme proxy route is not registered")
		return
	}

	s.logger.Info("received theme proxy request",
		"projectUser", route.ProjectUser,
		"projectPath", route.Request.ProjectPath,
		"port", route.Request.Port,
		"basePath", basePath,
		"upstreamPath", r.URL.Path,
	)

	s.proxyDevServer(w, r, route.Request, route.ProjectUser, devProxyUpstreamPath(basePath, r.URL.Path), r.URL.RawQuery)
}

func (s *Server) handleThemeRootProxy(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) && !s.isAllowedWebSocketOrigin(r) {
		writeError(w, http.StatusForbidden, "websocket origin is not allowed")
		return
	}

	route, basePath, ok := s.themeProxyRouteForRootRequest(r)
	if !ok {
		writeError(w, http.StatusNotFound, "theme proxy route is not registered")
		return
	}

	s.logger.Info("received theme root proxy request",
		"projectUser", route.ProjectUser,
		"projectPath", route.Request.ProjectPath,
		"port", route.Request.Port,
		"basePath", basePath,
		"upstreamPath", r.URL.Path,
		"referer", r.Header.Get("Referer"),
	)

	s.proxyDevServer(w, r, route.Request, route.ProjectUser, r.URL.Path, r.URL.RawQuery)
}

func (s *Server) proxyDevServer(w http.ResponseWriter, r *http.Request, req dev.RunRequest, projectUser, upstreamPath, upstreamQuery string) {
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
	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.StatusCode >= http.StatusBadRequest {
			s.logger.Warn("dev proxy upstream returned error",
				"projectUser", projectUser,
				"target", target.String(),
				"path", resp.Request.URL.Path,
				"status", resp.StatusCode,
			)
		}
		return rewriteDevProxyResponse(resp, req.DevServerBasePath)
	}
	proxy.ServeHTTP(w, r)
}

func devProxyUpstreamPath(basePath, requestPath string) string {
	basePath = normalizeProxyBasePath(basePath)
	if basePath == "" {
		return requestPath
	}

	for _, assetPath := range []string{
		"_build/",
		"api/theme-page/",
		"api/theme-watch/",
	} {
		fullPath := basePath + assetPath
		if requestPath == strings.TrimSuffix(fullPath, "/") || strings.HasPrefix(requestPath, fullPath) {
			return "/" + strings.TrimPrefix(requestPath, basePath)
		}
	}
	return requestPath
}

func rewriteDevProxyResponse(resp *http.Response, basePath string) error {
	basePath = normalizeProxyBasePath(basePath)
	if basePath == "" || resp.StatusCode == http.StatusSwitchingProtocols || resp.Body == nil {
		return nil
	}
	if !isRewritableDevProxyContent(resp.Header.Get("Content-Type")) {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()

	rewritten := rewriteDevProxyBody(body, basePath)
	resp.Body = io.NopCloser(bytes.NewReader(rewritten))
	resp.ContentLength = int64(len(rewritten))
	resp.Header.Set("Content-Length", strconv.Itoa(len(rewritten)))
	if !bytes.Equal(body, rewritten) {
		resp.Header.Del("ETag")
	}
	return nil
}

func isRewritableDevProxyContent(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	return strings.HasPrefix(contentType, "text/html") ||
		strings.HasPrefix(contentType, "text/css") ||
		strings.Contains(contentType, "javascript")
}

func rewriteDevProxyBody(body []byte, basePath string) []byte {
	basePath = normalizeProxyBasePath(basePath)
	if basePath == "" {
		return body
	}

	rewritten := append([]byte(nil), body...)
	for _, pathPrefix := range []string{
		"/_build",
		"/@vite/",
		"/@react-refresh",
		"/node_modules/.vite/",
		"/api/theme-page/",
		"/api/theme-watch/",
	} {
		rewritten = rewriteAbsoluteDevProxyPath(rewritten, pathPrefix, strings.TrimSuffix(basePath, "/")+pathPrefix)
	}
	rewritten = rewriteViteHMRPort(rewritten)
	return rewritten
}

func rewriteAbsoluteDevProxyPath(body []byte, fromPath, toPath string) []byte {
	for _, delimiter := range []string{"`", `"`, `'`, `(`, `url(`, `\"`, `\'`} {
		body = bytes.ReplaceAll(body, []byte(delimiter+fromPath), []byte(delimiter+toPath))
	}
	return body
}

func rewriteViteHMRPort(body []byte) []byte {
	return viteHMRPortPattern.ReplaceAll(body, []byte(`const hmrPort = importMetaUrl.port || (importMetaUrl.protocol === "https:" ? 443 : 80);`))
}

func normalizeProxyBasePath(basePath string) string {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" || basePath == "/" {
		return ""
	}
	if !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}
	if !strings.HasSuffix(basePath, "/") {
		basePath += "/"
	}
	return basePath
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
		case "projectUser", "projectPath", "port", "previewPath", "proxyPath", "previewUrl", "previewURL", "preview_url", "returnUrl", "returnURL", "return_url", "pageUrl", "pageURL", "page_url", "storeUUID", "storeUuid", "store_uuid", "installationID", "installationId", "installation_id":
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

func themeProxyBasePath(appPath string) (string, bool) {
	appPath = strings.TrimSpace(appPath)
	if appPath == "" {
		return "", false
	}
	if !strings.HasPrefix(appPath, "/") {
		appPath = "/" + appPath
	}
	parts := strings.Split(strings.Trim(appPath, "/"), "/")
	if len(parts) < 3 || parts[0] != "themes" {
		return "", false
	}
	return "/themes/" + parts[1] + "/" + parts[2] + "/", true
}

func (s *Server) ensureThemeProxyRouteAvailable(basePath, projectUser string) error {
	s.themeProxyMu.RLock()
	route, ok := s.themeProxyRoutes[basePath]
	s.themeProxyMu.RUnlock()
	if !ok || route.ProjectUser == projectUser {
		return nil
	}
	return config.ValidationError("theme proxy path " + basePath + " is already assigned to projectUser " + route.ProjectUser)
}

func (s *Server) registerThemeProxyRoute(basePath, projectUser string, req dev.RunRequest) error {
	s.themeProxyMu.Lock()
	defer s.themeProxyMu.Unlock()

	route, ok := s.themeProxyRoutes[basePath]
	if ok && route.ProjectUser != projectUser {
		return config.ValidationError("theme proxy path " + basePath + " is already assigned to projectUser " + route.ProjectUser)
	}
	s.themeProxyRoutes[basePath] = themeProxyRoute{
		ProjectUser: projectUser,
		Request:     req,
	}
	return nil
}

func (s *Server) themeProxyRouteForPath(requestPath string) (themeProxyRoute, string, bool) {
	basePath, ok := themeProxyBasePath(requestPath)
	if !ok {
		return themeProxyRoute{}, "", false
	}

	s.themeProxyMu.RLock()
	route, ok := s.themeProxyRoutes[basePath]
	s.themeProxyMu.RUnlock()
	return route, basePath, ok
}

func (s *Server) themeProxyRouteForRootRequest(r *http.Request) (themeProxyRoute, string, bool) {
	if referer := strings.TrimSpace(r.Header.Get("Referer")); referer != "" {
		if refererURL, err := url.Parse(referer); err == nil {
			if route, basePath, ok := s.themeProxyRouteForPath(refererURL.EscapedPath()); ok {
				return route, basePath, true
			}
		}
	}

	s.themeProxyMu.RLock()
	defer s.themeProxyMu.RUnlock()
	if len(s.themeProxyRoutes) != 1 {
		return themeProxyRoute{}, "", false
	}
	for basePath, route := range s.themeProxyRoutes {
		return route, basePath, true
	}
	return themeProxyRoute{}, "", false
}

func (s *Server) devRunProxyURLs(r *http.Request, projectUser, serverIP string) (devRunProxyURLs, error) {
	if projectUser == "" {
		return devRunProxyURLs{}, nil
	}
	appPath, appQueryFromURL, err := devProxyAppPathFromRequest(r)
	if err != nil {
		return devRunProxyURLs{}, err
	}
	appQuery := cleanDevProxyQuery(r.URL.Query()).Encode()
	if appQueryFromURL != "" {
		if appQuery != "" {
			appQuery = appQueryFromURL + "&" + appQuery
		} else {
			appQuery = appQueryFromURL
		}
	}
	basePath := devProxyBasePath(projectUser)
	isTheme := false
	if themeBasePath, ok := themeProxyBasePath(appPath); ok {
		basePath = themeBasePath
		isTheme = true
	}

	rootURL := s.absoluteServiceURL(r, serverIP, basePath)
	if appPath == "" {
		return devRunProxyURLs{
			AppURL:   rootURL,
			RootURL:  rootURL,
			BasePath: basePath,
			IsTheme:  isTheme,
		}, nil
	}

	appURL := devRunAppURL(rootURL, basePath, appPath)
	if appQuery != "" {
		appURL += "?" + appQuery
	}
	return devRunProxyURLs{
		AppURL:   appURL,
		RootURL:  rootURL,
		BasePath: basePath,
		IsTheme:  isTheme,
	}, nil
}

func devRunAppURL(rootURL, basePath, appPath string) string {
	if appPath == "" {
		return rootURL
	}

	relativePath := appPath
	basePath = normalizeProxyBasePath(basePath)
	if basePath != "" {
		basePathNoSlash := strings.TrimSuffix(basePath, "/")
		switch {
		case appPath == basePathNoSlash || appPath == basePath:
			relativePath = ""
		case strings.HasPrefix(appPath, basePath):
			relativePath = strings.TrimPrefix(appPath, basePath)
		case strings.HasPrefix(appPath, basePathNoSlash+"/"):
			relativePath = strings.TrimPrefix(appPath, basePathNoSlash+"/")
		}
	}

	if relativePath == "" {
		return rootURL
	}
	if !strings.HasPrefix(relativePath, "/") {
		relativePath = "/" + relativePath
	}
	return strings.TrimSuffix(rootURL, "/") + relativePath
}

func devProxyAppPathFromRequest(r *http.Request) (string, string, error) {
	appPath, appQuery, err := devProxyAppPathFromQuery(r.URL.Query())
	if err != nil || appPath != "" || appQuery != "" {
		return appPath, appQuery, err
	}

	referer := strings.TrimSpace(r.Header.Get("Referer"))
	if referer == "" {
		return "", "", nil
	}

	appPath, appQuery, err = normalizeDevProxyAppURL(referer)
	if err != nil {
		return "", "", err
	}
	if _, ok := themeProxyBasePath(appPath); !ok {
		return "", "", nil
	}
	return appPath, appQuery, nil
}

func devProxyAppPathFromQuery(values url.Values) (string, string, error) {
	for _, key := range []string{"previewPath", "proxyPath"} {
		if path := strings.TrimSpace(values.Get(key)); path != "" {
			return normalizeDevProxyAppPath(path)
		}
	}
	for _, key := range []string{"previewUrl", "previewURL", "preview_url", "returnUrl", "returnURL", "return_url", "pageUrl", "pageURL", "page_url"} {
		if rawURL := strings.TrimSpace(values.Get(key)); rawURL != "" {
			return normalizeDevProxyAppURL(rawURL)
		}
	}

	storeUUID := firstQueryValue(values, "storeUUID", "storeUuid", "store_uuid")
	installationID := firstQueryValue(values, "installationID", "installationId", "installation_id")
	if storeUUID == "" || installationID == "" {
		return "", "", nil
	}
	return "/themes/" + url.PathEscape(storeUUID) + "/" + url.PathEscape(installationID), "", nil
}

func normalizeDevProxyAppPath(path string) (string, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", "", nil
	}
	parsed, err := url.Parse(path)
	if err != nil {
		return "", "", err
	}
	if parsed.IsAbs() || parsed.Host != "" {
		return "", "", config.ValidationError("preview path must be a relative URL path")
	}

	normalized := parsed.EscapedPath()
	if normalized == "" {
		normalized = path
	}
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	return normalized, parsed.RawQuery, nil
}

func normalizeDevProxyAppURL(rawURL string) (string, string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", "", err
	}
	if parsed.Scheme == "" && parsed.Host == "" {
		return normalizeDevProxyAppPath(rawURL)
	}
	if parsed.Path == "" {
		return "", parsed.RawQuery, nil
	}
	path := parsed.EscapedPath()
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path, parsed.RawQuery, nil
}

func firstQueryValue(values url.Values, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(values.Get(key)); value != "" {
			return value
		}
	}
	return ""
}

func (s *Server) absoluteServiceURL(r *http.Request, serverIP, path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if host := devProxyHostFromServerIP(serverIP); host != "" {
		return "https://" + host + path
	}
	return requestScheme(r) + "://" + requestHost(r) + path
}

func hostFromAbsoluteURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func devProxyHostFromServerIP(serverIP string) string {
	serverIP = strings.TrimSpace(serverIP)
	if serverIP == "" {
		return ""
	}
	ip := net.ParseIP(serverIP)
	if ip == nil || ip.To4() == nil {
		return ""
	}
	return strings.ReplaceAll(serverIP, ".", "-") + "-heya-service.twalab.cloud"
}

func requestHost(r *http.Request) string {
	forwardedHost := strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if forwardedHost != "" {
		if comma := strings.Index(forwardedHost, ","); comma >= 0 {
			forwardedHost = strings.TrimSpace(forwardedHost[:comma])
		}
		return forwardedHost
	}
	return r.Host
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
