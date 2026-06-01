---
name: heya-solidjs-manager
description: Use when working in this repository on the Go microservice that starts and manages same-server SolidJS projects as local child processes.
---

# Heya SolidJS Manager

This repository is a Go HTTP microservice for managing SolidJS projects installed on the same server.

Current conventions:

- The executable lives under `cmd/heya-golang-microservice`.
- Internal packages stay under `internal/`.
- The HTTP service listens on `:8998` by default through `HEYA_HTTP_ADDR`.
- `/dev/run` is a WebSocket endpoint. The first connection starts a local SolidJS dev server by running `npm run dev -- --port 3002`; additional connections for the same project and port increment the internal connection count.
- When the last WebSocket connection for a project and port disconnects, stop the local process group.
- `/build/run` is a WebSocket endpoint. Each connection starts one local build with `npm run build`, streams output, and sends a final completion message.
- Keep project paths constrained by `HEYA_PROJECT_BASE_DIR`; do not accept arbitrary shell command input from API callers.
- Prefer environment variables for server-specific project, npm, process, and logging details.

Before changing behavior, run:

```bash
go test ./...
```
