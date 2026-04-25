# metacritic-harvester

A Go CLI for collecting public Metacritic list data into SQLite.

[中文说明](./docs/README_zh.md) | [Roadmap](./docs/roadmap.md) | [Usage](./docs/usage.md)

![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)

Current features:

- `crawl list`
- `crawl detail`
- `crawl reviews`
- `crawl batch`
- `crawl schedule`
- `detail query`
- `detail export`
- `detail compare`
- `latest query`
- `latest export`
- `latest compare`
- `review query`
- `review export`
- `review compare`
- filters for year, platform, network, genre, and release type
- snapshot history in `list_entries`
- current-state view in `latest_list_entries`
- per-task `run_id` tracking in `crawl_runs`
- current detail view in `work_details`
- immutable detail history in `work_detail_snapshots`
- detail fetch tracking in `detail_fetch_state`
- current review view in `latest_reviews`
- immutable review history in `review_snapshots`
- review fetch tracking in `review_fetch_state`

## Quick start

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --source=api --pages=1 --db=output/metacritic.db
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=game --source=api
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=game --review-type=critic --limit=10
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --category=game
```

Batch example:

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-tasks.yaml --concurrency=2
```

Latest compare example:

```bash
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b>
```

Latest summary export example:

```bash
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --run-id=<run-a> --profile=summary --format=csv --output=output/latest-summary.csv
```

Detail compare example:

```bash
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b>
```

Detail flat export example:

```bash
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --run-id=<detail-run-a> --profile=flat --format=csv --output=output/detail-flat.csv
```

Review summary export example:

```bash
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --run-id=<review-run-a> --profile=summary --format=json --output=output/review-summary.json
```

Docs:

- [Chinese overview](./docs/README_zh.md)
- [Roadmap](./docs/roadmap.md)
- [Usage](./docs/usage.md)
- [Batch tasks](./docs/batch-tasks.md)
- [Latest commands](./docs/latest.md)
- [Detail query/export/compare](./docs/usage.md#detail-query)
- [Scheduling](./docs/scheduling.md)
- [Filters](./docs/filters.md)

## Tooling

Install `sqlc` before regenerating database code:

```bash
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate
```
