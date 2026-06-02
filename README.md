# Heya Golang Microservice

Small Go service for starting and later managing SolidJS projects installed on the same server. The service listens on `:8998` by default and exposes WebSocket endpoints for dev and build workflows.

## Run

```bash
go run ./cmd/heya-golang-microservice
```

## Configuration

Copy `.env.example` into your server environment and set the values you need. The service starts SolidJS projects as local child processes; no SSH server is required.

Important values:

- `HEYA_HTTP_ADDR`: HTTP bind address, default `:8998`
- `HEYA_PROJECT_BASE_DIR`: base directory that project paths must stay inside
- `HEYA_DEFAULT_PROJECT_DIR`: optional default SolidJS project directory
- `HEYA_DEFAULT_DEV_PORT`: default SolidJS dev port, default `3002`
- `HEYA_DEV_SERVER_HOST`: host returned to WebSocket clients, default `localhost`
- `HEYA_DEV_SERVER_BIND_HOST`: host passed to Vite with `--host`, default `0.0.0.0`; `DEV_SERVER_HOST` is also accepted as a fallback
- `HEYA_DEV_SERVER_SCHEME`: scheme returned to WebSocket clients, default `http`
- `HEYA_DEV_READY_HOST`: host used by the service to check local dev server readiness, default `localhost`
- `HEYA_WEBSOCKET_ALLOWED_ORIGINS`: comma-separated browser origins allowed to open WebSockets, default `https://admin.thewebaddicts.com`
- `HEYA_NPM_BIN`: npm command used inside the shell, default `npm`
- `HEYA_COMMAND_SHELL`: shell used to launch npm, default your `$SHELL` or `/bin/zsh`
- `HEYA_DEV_READY_TIMEOUT`: max time to wait for the dev server URL to respond before returning an error, default `60s`
- `HEYA_DEV_IDLE_TIMEOUT`: time to keep a dev server alive after the last WebSocket disconnects, default `30s`
- `HEYA_LOG_DIR`: local directory for dev server logs
- `HEYA_BUILD_ROOT_DIR`: local directory for isolated safe build workspaces, default `/tmp/heya-builds`
- `HEYA_ACCOUNT_INFO_URL`: DevOps account lookup endpoint, default `https://devops.twalab.live/api/v2/theme-builder/account/info`
- `HEYA_ACCOUNT_INFO_TOKEN`: token sent as the `token` header for account lookup
- `HEYA_ACCOUNT_INFO_TIMEOUT`: timeout for account lookup requests, default `10s`
- `HEYA_PROCESS_STOP_TIMEOUT`: how long to wait before force-killing the process group

Check npm before using the API:

```bash
node -v && npm -v
```

The service launches the dev command through a login shell, from the project directory. That keeps Herd/NVM/Homebrew setup behavior close to what happens when you type the command in Terminal.

```bash
export HEYA_COMMAND_SHELL=/bin/zsh
export HEYA_NPM_BIN=npm
```

## WebSocket API

Open a WebSocket connection:

```text
ws://localhost:8998/dev/run?projectUser=energybri_6a19492405faf
```

To open a specific app route through the proxy, pass `previewPath`:

```text
ws://localhost:8998/dev/run?projectUser=energybri_6a19492405faf&previewPath=/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6&preview=true
```

If the caller already has a full preview URL, pass it as `previewUrl`. The service also accepts `pageUrl` when the browser already knows the current admin page URL. If none of those values are provided, the service falls back to the WebSocket request's `Referer` header when it contains a `/themes/{store uuid}/{installation id}` path.

The service also accepts `storeUUID` plus `installationID` and builds the matching `/themes/{store uuid}/{installation id}` path.

When `projectUser` is provided, the service first resolves the account through:

```text
POST https://devops.twalab.live/api/v2/theme-builder/account/info
Header: token=<HEYA_ACCOUNT_INFO_TOKEN>
Body: {"account":"<projectUser>"}
```

It then uses `working_directory_heya` as the project directory and `account.port_dev_live` as the dev port. For `projectUser` requests, the returned `devServerURL` points to the public microservice proxy URL, not the raw dev-server port.

The old direct call still works as a fallback:

```text
ws://localhost:8998/dev/run?projectPath=/Library/WebServer/Documents/abc/storage/app/frontend&port=3002
```

When the first connection opens, the service runs this command locally from the resolved project directory:

```bash
npm run dev -- --host 0.0.0.0 --port <account.port_dev_live> --base <proxy base path>
```

After the dev server responds over HTTP, the service sends a JSON message containing the dev server URL, current connection count, local PID, and log file path:

```json
{
  "type": "dev_server",
  "status": "running",
  "devServerURL": "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6?preview=true",
  "devProxyURL": "https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/",
  "connections": 1
}
```

Additional WebSocket connections for the same `projectPath` and `port` increment an internal count and reuse the running dev server. When the last connection disconnects, the service stops the local process group.

## Dev Server Proxy

The microservice can expose many local Vite dev servers through one public HTTP(S) entrypoint instead of opening each dev-server port. When `/dev/run` receives theme context, the service uses a route-native proxy URL:

```text
https://<service-host>/themes/<storeUUID>/<installationID>/...
```

For example, a dev server on local port `12036` can be exposed as:

```text
https://91-98-82-198-heya-service.twalab.cloud/themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/
```

The proxy registers that theme base path to the resolved `projectUser` and forwards requests internally to `HEYA_DEV_READY_HOST:<account.port_dev_live>`. Vite starts with the same base path, such as:

```bash
npm run dev -- --host 0.0.0.0 --port 12036 --base /themes/57726969-9e2e-11ed-9f8e-42010a960004/z-6a1ef6c3dcca6/
```

Multiple projects on the same server are separated by path, not public port:

```text
/themes/store-a/install-a/ -> http://localhost:12036
/themes/store-b/install-b/ -> http://localhost:12037
```

The same theme base path cannot be assigned to a different `projectUser` while it is already registered.

If no theme context is provided, the project-scoped proxy URL remains available as a fallback:

```text
https://<service-host>/dev/proxy/<projectUser>/
```

For example:

```text
https://91-98-82-198-heya-service.twalab.cloud/dev/proxy/heyasite_6a1ef2a3528e1/
```

The proxy resolves `projectUser`, starts or reuses the local dev server on the account's assigned port, and forwards requests internally to `HEYA_DEV_READY_HOST:<port>` without requiring that port to be public. Project-user dev runs start Vite with a matching base path such as `--base /dev/proxy/heyasite_6a1ef2a3528e1/` so Vite asset and HMR paths stay under the proxy prefix.

## Build API

Open a WebSocket connection:

```text
ws://localhost:8998/build/run?projectUser=energybri_6a19492405faf&mode=safe
```

When `projectUser` is provided, the service resolves `working_directory_heya` and builds from that directory. The old direct `projectPath` query is still supported as a fallback.

The first connection starts one shared build job. Later connections for the same resolved project path and `mode` attach to that running job instead of starting another `npm run build`. By default, `mode=safe` copies the selected project into an isolated temporary workspace under `HEYA_BUILD_ROOT_DIR`, excludes live/cache folders such as `node_modules`, `.git`, `.output`, `dist`, and `.vinxi`, runs install there, then runs:

```bash
npm run build
```

The live project directory is not mutated in safe mode. Use `mode=live` only if you explicitly want to build in the source directory.

To resume status after a browser refresh without starting a new build, connect with `watch=true`:

```text
ws://localhost:8998/build/run?projectUser=energybri_6a19492405faf&mode=safe&watch=true
```

The service streams build messages:

```json
{
  "type": "build_status",
  "status": "building",
  "running": true,
  "attached": true
}
```

```json
{
  "type": "build_started",
  "build": {
    "projectPath": "/Library/WebServer/Documents/abc/storage/app/frontend",
    "mode": "safe",
    "buildProjectPath": "/tmp/heya-builds/frontend-123456",
    "command": "'npm' run build",
    "pid": "12345"
  }
}
```

```json
{
  "type": "build_output",
  "stream": "stdout",
  "data": "vinxi build"
}
```

```json
{
  "type": "build_complete",
  "status": "success",
  "exitCode": 0
}
```

If a WebSocket disconnects before completion, the build continues and another WebSocket can attach while it is still running.
