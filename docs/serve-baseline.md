# Serve Baseline

This note captures the decisions made before implementing `serve` in Phase 6.

## Default security baseline

- Bind to `127.0.0.1` by default.
- Expose read-only operations by default:
  - health checks
  - read-only query endpoints
  - read-only export endpoints
- Keep write operations disabled by default:
  - `crawl list/detail/reviews`
  - batch execution
  - schedule create/update/delete
  - server-side export-to-file operations
- If write operations are enabled later, only allow local access in the default baseline.
- Phase 6 does not assume built-in multi-user auth and does not assume public exposure.

## Export path rules

- Do not allow arbitrary absolute output paths through HTTP.
- Limit export file writes to a controlled directory, recommended: `output/exports`.
- Do not allow API-triggered writes to overwrite arbitrary files inside the repository.

## Observability model

Phase 6 should reuse existing data sources instead of creating new observability tables first.

- Recent runs:
  - source: `crawl_runs`
- Current fetch state:
  - source: `detail_fetch_state`
  - source: `review_fetch_state`
- Aggregated result summaries:
  - source: existing `latest/detail/review export --profile=summary` logic

Recommended minimum read models for the service layer:

- `RunStatusView`
  - `run_id`
  - `source`
  - `task_name`
  - `category`
  - `filter_key`
  - `status`
  - `started_at`
  - `finished_at`
  - `error`
- `RunOutcomeView`
  - list: page/write counts and fallback diagnostics
  - detail: processed/fetched/skipped/failed, enrich counts, fallback diagnostics
  - review: candidates/scopes/reviews/failures, fixed `source=api`
- `FetchStateView`
  - `status`
  - `last_attempted_at`
  - `last_fetched_at`
  - `last_run_id`
  - `last_error_type`
  - `last_error_stage`

These should be implemented as read-only aggregation in repository/service code first. Only add materialized views or new tables later if real Phase 6 complexity or performance demands it.

## SQLite boundary for Phase 6

- Keep SQLite as the default Phase 6 storage.
- Continue to use WAL mode.
- Keep explicit checkpoint capability.
- Do not allow multiple write-heavy tasks to hit the same database without strict gating.
- Treat read-mostly / write-light workloads as the intended operating mode.
- If Phase 6 reveals sustained high-frequency concurrent writes, evaluate heavier storage abstractions in a later phase instead of preemptively adding them now.

## Non-functional checks to carry into Phase 6

- Observe WAL growth under longer-running usage.
- Check whether long read queries plus write tasks cause `SQLITE_BUSY`.
- Verify that checkpointing can bring WAL size back to an acceptable range.
- Keep HTTP handlers separate from CLI formatting; reuse repository/service code, not CLI output code.
