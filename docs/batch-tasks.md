# Batch tasks

`crawl batch` executes a YAML file containing multiple crawl tasks.

## Commands

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-tasks.yaml
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-concurrent.yaml --concurrency=2
```

## Supported task kinds

- `list`
- `detail`
- `reviews`

If `kind` is omitted, the batch parser treats the task as `list` for backward compatibility.

## YAML shape

```yaml
defaults:
  db: output/metacritic.db
  retries: 3
  debug: false
  concurrency: 2

tasks:
  - kind: list
    name: game-metascore-pc
    category: game
    metric: metascore
    pages: 2
    platform: [pc, ps5]

  - kind: detail
    name: game-detail-refresh
    category: game
    limit: 20
    source: api

  - kind: reviews
    name: critic-reviews
    category: movie
    review-type: critic
    limit: 10
```

## Defaults

`defaults` can hold shared values such as:

- `db`
- `retries`
- `debug`
- `proxies`
- `concurrency`

Task-level fields override `defaults`.

## Notes

- batch is still file-driven and CLI-driven
- batch execution reuses the same services as direct CLI commands
- writes to the same SQLite file are gated to avoid `SQLITE_BUSY`
- if you want meaningful write-side concurrency, split tasks across different databases

## Output

Each task produces its own summary and run lineage.

At the end, the batch command prints an aggregate summary across all tasks.
