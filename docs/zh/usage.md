# 使用方式

## 环境要求

- Go 1.26+
- 可以访问公开的 Metacritic 页面和接口
- 如果要重新生成数据库访问代码，需要安装 `sqlc`

```bash
go mod tidy
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate
```

## 命令总览

```bash
go run ./cmd/metacritic-harvester --help
go run ./cmd/metacritic-harvester crawl --help
go run ./cmd/metacritic-harvester latest --help
go run ./cmd/metacritic-harvester detail --help
go run ./cmd/metacritic-harvester review --help
go run ./cmd/metacritic-harvester serve --help
```

当前已实现：

- `crawl list`
- `crawl detail`
- `crawl reviews`
- `crawl batch`
- `crawl schedule`
- `serve`
- `latest query / export / compare`
- `detail query / export / compare`
- `review query / export / compare`

## crawl list

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --pages=0 --db=output/metacritic.db
go run ./cmd/metacritic-harvester crawl list --category=movie --metric=userscore --year=2011:2014 --network=netflix,max --genre=drama,thriller
go run ./cmd/metacritic-harvester crawl list --category=tv --metric=newest --source=auto --pages=2
```

常用参数：

- `--category=game|movie|tv`
- `--metric=metascore|userscore|newest`
- `--source=api|html|auto`
- `--year=YYYY:YYYY`
- `--platform=...` 仅 `game`
- `--network=...` 仅 `movie|tv`
- `--genre=...`
- `--release-type=...` 仅 `game|movie`
- `--pages`
- `--db`
- `--retries`
- `--proxies`
- `--debug`

说明：

- 默认 source 是 `api`
- `--pages=0` 表示抓取全部榜单页
- `--source=html` 会强制使用旧的 HTML 路径
- `--source=auto` 表示先尝试 API，失败后回退到 HTML
- list 的 fallback 粒度是整次 run

## crawl detail

```bash
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=game --limit=20
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --work-href=https://www.metacritic.com/game/baldurs-gate-3/ --source=auto
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=tv --force
```

常用参数：

- `--category=game|movie|tv`
- `--work-href`
- `--limit`
- `--force`
- `--concurrency`
- `--source=api|html|auto`
- `--db`
- `--retries`
- `--proxies`
- `--debug`

说明：

- detail 默认走 `api`
- `--limit=0` 表示处理全部详情候选作品
- `--work-href` 支持绝对 URL，也支持 `/game/...` 这种相对路径
- detail 的 `auto` fallback 粒度是单作品
- API 路径会在需要时用 HTML/Nuxt 补 `where_to_buy`、`where_to_watch`
- enrich 失败不会把主详情成功的作品记成失败

## crawl reviews

```bash
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=game --review-type=critic --limit=10
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=movie --review-type=user --limit=10
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --work-href=https://www.metacritic.com/tv/shogun-2024/ --review-type=all --force
```

常用参数：

- `--category=game|movie|tv`
- `--review-type=critic|user|all`
- `--work-href`
- `--platform`
- `--limit`
- `--page-size`
- `--max-pages`
- `--concurrency`
- `--force`
- `--db`
- `--retries`
- `--proxies`
- `--debug`

说明：

- reviews 采用 `API-first`
- `--limit=0` 表示处理全部评论候选作品
- 评论快照写入 `review_snapshots`
- 当前视图写入 `latest_reviews`
- 恢复粒度是 `work_href + review_type + platform_key`

## crawl batch

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-tasks.yaml
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-concurrent.yaml --concurrency=2
```

详见：[批量任务](./batch-tasks.md)

## crawl schedule

```bash
go run ./cmd/metacritic-harvester crawl schedule --file=examples/schedule-jobs.yaml
```

详见：[调度说明](./scheduling.md)

## latest query / export / compare

```bash
go run ./cmd/metacritic-harvester latest query --db=output/metacritic.db --category=game --metric=metascore --limit=10
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --format=csv --output=output/latest.csv
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --run-id=<run-id> --profile=summary --format=json --output=output/latest-summary.json
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b>
```

详见：[榜单读侧](./latest.md)

## detail query / export / compare

```bash
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --category=game --format=json
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --profile=flat --format=csv --output=output/detail-flat.csv
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --run-id=<detail-run-id> --profile=summary --format=json --output=output/detail-summary.json
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b>
```

说明：

- `detail query` 读取当前 `work_details`
- `detail export --run-id` 读取 `work_detail_snapshots`
- `flat` 会把常用扩展字段摊平成适合 CSV 的列
- `summary` 返回聚合后的覆盖率摘要

## review query / export / compare

```bash
go run ./cmd/metacritic-harvester review query --db=output/metacritic.db --category=game --review-type=critic --format=json
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --profile=flat --format=csv --output=output/review-flat.csv
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --run-id=<review-run-id> --profile=summary --format=json --output=output/review-summary.json
go run ./cmd/metacritic-harvester review compare --db=output/metacritic.db --from-run-id=<review-run-a> --to-run-id=<review-run-b>
```

说明：

- `raw` 保留 `source_payload_json`
- `flat` 保留标准化列，去掉 payload 噪音
- `summary` 按 run、类别、评论类型、平台聚合

## serve

纯后端模式：

```bash
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db
```

嵌入式全栈控制台：

```bash
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db --full-stack --enable-write
```

说明：

- 默认地址是 `127.0.0.1:36666`
- 默认只读
- 写操作需要 `--enable-write`
- 实时日志流地址是 `/api/logs/stream`
- 支持 `latest / detail / review` 的浏览器下载导出
- batch 和 schedule 仍然保留在 CLI

详见：[Serve](./serve.md)

## 数据模型摘要

主流程当前会使用这些持久化表：

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

## 测试

```bash
go test ./...
```
