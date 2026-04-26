# Serve

`serve` starts a local HTTP runtime on top of the existing repository, storage, and app services.

It supports two modes:

- backend-only
- full-stack with an embedded Vue control panel

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
127.0.0.1:36666
```

## Flags

- `--addr`
- `--db`
- `--full-stack`
- `--enable-write`
- `--base-url`
- `--backend-base-url`

## Security baseline

- binds to loopback by default
- read-only by default
- write endpoints require `--enable-write`
- write requests are still limited to loopback in the default baseline

See [Serve baseline](./serve-baseline.md).

## Read endpoints

- `GET /healthz`
- `GET /api/config`
- `GET /api/runs`
- `GET /api/overview`
- `GET /api/tasks`
- `GET /api/tasks/{id}`
- `GET /api/logs`
- `GET /api/logs/stream`
- `GET /api/latest`
- `GET /api/detail`
- `GET /api/review`
- `GET /api/export/latest`
- `GET /api/export/detail`
- `GET /api/export/review`
- `GET /api/detail/state`
- `GET /api/review/state`

## Write endpoints

- `POST /api/tasks/list`
- `POST /api/tasks/detail`
- `POST /api/tasks/reviews`

## Export downloads

The service exposes browser-friendly download endpoints for:

- `latest`
- `detail`
- `review`

Query parameters stay close to the CLI export commands:

- `format=csv|json`
- `profile=raw|flat|summary`
- `run_id` for snapshot exports
- the same category/work/platform/filter-style selectors already supported by each export

Downloads stream directly to the client and do not write files on the server.

## Current web-console scope

The embedded console currently focuses on:

- launching `list / detail / reviews`
- viewing recent runs
- viewing in-process tasks
- downloading exports
- watching live crawl logs

Batch and schedule execution remain CLI-driven. The console documents that workflow instead of triggering it over HTTP.
