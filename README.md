# metacritic-harvester

A local-first Go toolkit for collecting public Metacritic list, detail, and review data into SQLite.

[中文说明](./docs/zh/README.md) | [Usage](./docs/usage.md) | [Serve](./docs/serve.md)

![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Release](https://img.shields.io/github/v/release/gofurry/metacritic-harvester?style=flat&color=blue)
[![Go Report Card](https://goreportcard.com/badge/github.com/gofurry/metacritic-harvester)](https://goreportcard.com/report/github.com/gofurry/metacritic-harvester)

## Successful Crawl Snapshot

The following screenshot shows a successfully generated compressed package containing the complete harvested dataset.

![Successful crawl package snapshot](./docs/260509.png)

## What it does

`metacritic-harvester` is built for collecting and working with public Metacritic data locally.

It currently supports:

- list crawling for games, movies, and TV
- work detail crawling
- critic and user review crawling
- SQLite-backed current-state views and immutable snapshots
- query, export, and compare commands for `latest`, `detail`, and `review`
- `API-first` crawling with HTML fallback where it still helps
- local batch and schedule execution from the CLI
- a local `serve` runtime with an embedded operations console

## Core commands

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
- `crawl list`, `crawl detail`, and `crawl reviews` support `--timeout`
- `crawl list`, `crawl detail`, and `crawl reviews` support `--continue-on-error`
- `crawl list`, `crawl detail`, and `crawl reviews` support `--rps` and `--burst`
- default source is `api`
- default crawl timeout is `3h`
- default crawl rate limit is `2 RPS` with `burst=2`
- `--continue-on-error=true` by default for list, detail, and reviews
- `auto` means "try API first, then fall back on failure"
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

Default crawl semantics:

- `pages=0` means crawl all list pages
- `limit=0` means process all detail or review candidates
- `--concurrency` controls worker count while `--rps` / `--burst` control the shared request limiter
- partial crawl failures are counted in the summary without failing the command unless `--continue-on-error=false`
- command-level timeout is `3h` unless overridden with `--timeout`

## Release builds

Use the root-level build script to create precompiled binaries:

```bat
build.bat
```

Artifacts are written to:

- `output/releases`

Targets included:

- `windows/amd64`
- `windows/arm64`
- `linux/amd64`
- `linux/arm64`
- `darwin/amd64`
- `darwin/arm64`

Build output names follow this pattern:

- `metacritic-harvester_windows_amd64.exe`
- `metacritic-harvester_linux_arm64`

The script uses size-focused Go build flags:

- `-trimpath`
- `-ldflags "-s -w -buildid="`
- `CGO_ENABLED=0`

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
