# Roadmap

## Current status

For the current local-first scope, the project is feature-complete through the first edition of Phase 6.

The core system now includes:

- list, detail, and review crawling
- current-state views plus immutable snapshots
- read-side query, export, and compare flows
- `API-first` fetching with controlled fallback
- batch and schedule execution in the CLI
- a local `serve` runtime with an embedded operations console

The remaining work is mostly service-runtime refinement and product polish, not missing core capability.

## Data model status

Current core tables:

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

This is already enough to support:

- current-state queries
- historical snapshots
- `run_id`-based comparison
- export and summary views

## Phase 1

Status: completed

Delivered:

- Cobra CLI foundation
- SQLite + `sqlc`
- `crawl list`
- `game / movie / tv`
- `metascore / userscore / newest`

## Phase 2

Status: completed

Delivered:

- list filters
- `latest_list_entries`
- `crawl batch`
- `crawl schedule`
- `crawl_runs`
- `latest query / export / compare`

## Phase 2.5

Status: completed

Delivered:

- stability and consistency hardening
- transactional list writes
- WAL / checkpoint support
- safer batch and schedule execution semantics

## Phase 3

Status: completed

Delivered:

- `crawl detail`
- `work_details`
- `detail_fetch_state`
- game / movie / tv detail crawling

## Phase 3.5

Status: completed

Delivered:

- `work_detail_snapshots`
- detail run lineage, recovery, and diagnostics
- `detail query / export / compare`
- detail in batch / schedule
- `where_to_buy / where_to_watch` extraction

## Phase 4

Status: completed

Delivered:

- `crawl reviews`
- `review query / export / compare`
- reviews moved to `API-first`
- `latest_reviews + review_snapshots`
- `review_fetch_state`
- scope-level recovery and normalization

## Phase 5

Status: completed

Delivered:

- `--run-id` snapshot exports
- `--profile=raw|flat|summary`
- richer export shapes for analysis and BI

## Phase 5-1

Status: completed

Delivered:

- `crawl list --source=api|html|auto`
- `crawl detail --source=api|html|auto`
- list finder API adapter
- detail composer API adapter
- `API-first` main path
- `auto` fallback and detail enrich rules

## Phase 5.5

Status: completed

Delivered:

- unified default source semantics
- shared runtime protection
- structured fallback and enrich diagnostics
- finder mapping consolidation
- opt-in benchmark / soak hooks
- `serve` baseline decisions for security, observability, and SQLite boundaries

## Phase 6

Status: in progress, first edition complete

Delivered:

- `serve`
- local HTTP API
- embedded operations console
- live crawl logs
- local task launching for list, detail, and reviews
- browser download exports

Current boundary:

- loopback-only by default
- read-only by default
- write operations require explicit enablement
- batch and schedule remain CLI-driven

## Recommended next focus

1. continue refining the Phase 6 service experience
2. only evaluate heavier auth, observability, or storage changes if real usage proves the need
