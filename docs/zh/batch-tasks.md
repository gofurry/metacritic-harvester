# 批量任务

`crawl batch` 用来执行包含多个抓取任务的 YAML 文件。

## 命令

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-tasks.yaml
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-concurrent.yaml --concurrency=2
```

## 支持的任务类型

- `list`
- `detail`
- `reviews`

如果 `kind` 省略，出于兼容性会按 `list` 处理。

## YAML 结构

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

## defaults

`defaults` 可以放这些共享字段：

- `db`
- `retries`
- `debug`
- `proxies`
- `concurrency`

任务级字段会覆盖 `defaults`。

## 说明

- batch 仍然是文件驱动、CLI 驱动
- batch 执行会复用和单命令抓取相同的 service
- 对同一个 SQLite 文件的写入会做 gate，避免 `SQLITE_BUSY`
- 如果你希望真正获得写侧并发收益，建议拆到不同数据库

## 输出

每个任务都会有自己的摘要和 `run_id`。

最后 batch 命令会给出整体聚合摘要。
