# Scheduling

`crawl schedule` starts a foreground local scheduler that triggers batch files by cron expression.

## Command

```bash
go run ./cmd/metacritic-harvester crawl schedule --file=examples/schedule-jobs.yaml
```

## Schedule file shape

```yaml
timezone: Asia/Shanghai

jobs:
  - name: morning-game
    cron: "0 9 * * *"
    batch_file: ../examples/batch-tasks.yaml
    enabled: true
    concurrency: 2

  - name: nightly-tv
    cron: "0 1 * * *"
    batch_file: ../examples/batch-concurrent.yaml
    enabled: true
```

## Supported fields

Top level:

- `timezone`
- `jobs`

Each job:

- `name`
- `cron`
- `batch_file`
- `enabled`
- `concurrency`

## Runtime behavior

- the scheduler rereads the target batch YAML when a job fires
- each underlying crawl task still gets its own `run_id`
- the scheduler is local and foreground by design
- on interrupt, it stops taking new jobs and lets in-flight work finish

## Notes

- schedule is CLI-only
- if multiple jobs write to the same SQLite file, writes are still gated
- use small jobs first before relying on a long-running schedule
