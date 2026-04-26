# Serve

`serve` starts a local HTTP server on top of the existing repository, storage, and app services.

It supports two modes:

- backend-only: JSON API only
- full-stack: JSON API plus an embedded lightweight control panel

## Quick start

Backend only:

```bash
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db
```

Full-stack console:

```bash
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db --full-stack --enable-write
```

Default address:

```text
127.0.0.1:8080
```

## Flags

- `--addr`
- `--db`
- `--full-stack`
- `--enable-write`
- `--base-url`
- `--backend-base-url`

## Security baseline

- the server binds to loopback by default
- write endpoints are disabled by default
- when write endpoints are enabled, they still only accept loopback requests
- the backend mode exposes API endpoints only

## API overview

Read endpoints:

- `GET /healthz`
- `GET /api/config`
- `GET /api/runs`
- `GET /api/tasks`
- `GET /api/tasks/{id}`
- `GET /api/logs`
- `GET /api/logs/stream`
- `GET /api/latest`
- `GET /api/detail`
- `GET /api/review`
- `GET /api/detail/state`
- `GET /api/review/state`

Write endpoints:

- `POST /api/tasks/list`
- `POST /api/tasks/detail`
- `POST /api/tasks/reviews`

## Notes

- the full-stack UI is intentionally small and uses embedded static assets
- live crawl logs are exposed through Server-Sent Events at `/api/logs/stream`
- task execution reuses the existing app services, so crawl logging and run tracking stay consistent with the CLI
