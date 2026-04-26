# metacritic-harvester

A local-first Go toolkit for collecting public Metacritic list, detail, and review data into SQLite.

[中文说明](./docs/zh/README.md) | [Roadmap](./docs/roadmap.md) | [Usage](./docs/usage.md) | [Serve](./docs/serve.md)

![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Release](https://img.shields.io/github/v/release/GoFurry/metacritic-harvester?style=flat&color=blue)
[![Go Report Card](https://goreportcard.com/badge/github.com/GoFurry/metacritic-harvester)](https://goreportcard.com/report/github.com/GoFurry/metacritic-harvester)

## Project status

For the current roadmap scope, the project is feature-complete through the first edition of Phase 6.

What that means in practice:

- list, detail, and review crawling are all implemented
- current-state views and immutable snapshots are implemented
- query, export, and compare flows are implemented for `latest`, `detail`, and `review`
- `API-first` is implemented, with HTML/Nuxt fallback where it still makes sense
- batch and schedule execution are available from the CLI
- a local `serve` runtime and embedded operations console are available

The remaining work is mostly product polish and service-runtime refinement, not missing core capability.

## Current feature set

- `crawl list`
- `crawl detail`
- `crawl reviews`
- `crawl batch`
- `crawl schedule`
- `serve`
- `latest query / export / compare`
- `detail query / export / compare`
- `review query / export / compare`

Storage and history:

- `works`
- `crawl_runs`
- `list_entries`
- `latest_list_entries`
- `work_details`
- `work_detail_snapshots`
- `detail_fetch_state`
- `latest_reviews`
- `review_snapshots`
- `review_fetch_state`

Runtime behavior:

- `crawl list` and `crawl detail` support `--source=api|html|auto`
- default source is `api`
- `auto` means “try API first, then fall back on failure”
- detail enrich keeps HTML/Nuxt only for fields the API path does not fully cover yet
- browser download exports are available in `serve`
- batch and schedule remain CLI-driven even when the web console is enabled

## Quick start

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --source=api --pages=0 --db=output/metacritic.db
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=game --source=api
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=game --review-type=critic
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --category=game
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db --full-stack --enable-write
```

## Serve highlights

The built-in local service supports:

- local JSON API
- embedded Vue control panel
- live crawl logs via SSE
- local task launching for list, detail, and reviews
- browser download exports for `latest / detail / review`

Current serve boundary:

- binds to `127.0.0.1` by default
- read-only by default
- write operations require `--enable-write`
- batch and schedule execution stay on the CLI; the console documents that workflow instead of triggering it over HTTP

## Recommended docs

- [Usage](./docs/usage.md)
- [Serve](./docs/serve.md)
- [Batch tasks](./docs/batch-tasks.md)
- [Scheduling](./docs/scheduling.md)
- [Filters](./docs/filters.md)
- [Latest commands](./docs/latest.md)
- [Roadmap](./docs/roadmap.md)

## Tooling

Install `sqlc` before regenerating database code:

```bash
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate
```

Run tests:

```bash
go test ./...
```
