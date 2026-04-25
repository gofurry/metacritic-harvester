# 使用方式

## 环境要求

- Go 1.26+
- 可访问 Metacritic 的网络环境
- 如需重新生成数据库访问代码，需要安装 `sqlc`

```bash
go mod tidy
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate
```

Windows 下如果 `sqlc` 不在 `PATH` 中：

```powershell
$env:USERPROFILE\go\bin\sqlc.exe generate
```

## 命令总览

查看帮助：

```bash
go run ./cmd/metacritic-harvester --help
go run ./cmd/metacritic-harvester crawl --help
go run ./cmd/metacritic-harvester detail --help
go run ./cmd/metacritic-harvester latest --help
go run ./cmd/metacritic-harvester review --help
```

当前已实现的命令：

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

## crawl list

基础示例：

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --pages=1 --db=output/metacritic.db
```

过滤示例：

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --year=2011:2014 --platform=pc,ps5 --genre=action,rpg --release-type=coming-soon
go run ./cmd/metacritic-harvester crawl list --category=movie --metric=userscore --year=2011:2014 --network=netflix,max --genre=drama,thriller --release-type=coming-soon,in-theaters
go run ./cmd/metacritic-harvester crawl list --category=tv --metric=newest --year=2011:2014 --network=hulu,netflix --genre=drama,thriller
```

常用参数：

- `--category=game|movie|tv`
- `--metric=metascore|userscore|newest`
- `--source=api|html|auto`，默认 `api`
- `--year=YYYY:YYYY`
- `--platform=...` 仅 `game`
- `--network=...` 仅 `movie|tv`
- `--genre=...`
- `--release-type=...` 仅 `game|movie`
- `--pages`
- `--db`
- `--debug`
- `--retries`
- `--proxies`

命令完成后会输出当前 `run_id`，可以直接用于 `latest compare`。

说明：

- `crawl list` 默认走后端 finder API
- `--source=html` 会强制使用现有 HTML 抓取路径
- `--source=auto` 会先尝试 API，失败后整次任务回退到 HTML

## crawl detail

基于已有 `works.href` 抓详情页：

```bash
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db
```

常见用法：

```bash
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=game
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=movie --limit=20
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --work-href=https://www.metacritic.com/game/baldurs-gate-3
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=tv --force
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --source=auto --category=movie
```

说明：

- 详情页来源于 `crawl list` 已写入的 `works.href`
- `crawl detail` 默认走 composer API 作为主数据源
- 默认跳过已经成功抓取过的作品
- `--force` 会重新抓取已成功详情
- `--work-href` 支持传完整 URL，也支持以 `/game/...`、`/movie/...`、`/tv/...` 开头的相对路径
- `--source=html` 会强制使用现有 HTML 详情路径
- `--source=auto` 会先尝试 API，单个作品 API 主抓失败时回退到 HTML
- 当前在 `source=api` 下会用 HTML / Nuxt 做 enrich，补充：
  - `game` 的 `where_to_buy`
  - `movie / tv` 的 `where_to_watch`

常用参数：

- `--category=game|movie|tv`
- `--work-href`
- `--limit`
- `--force`
- `--db`
- `--debug`
- `--retries`
- `--proxies`

## crawl reviews

基于 Metacritic 后端公开接口抓取评论：

```bash
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=game --limit=10
```

常见用法：

```bash
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=game --review-type=critic --platform=pc --limit=5
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=movie --review-type=user --limit=10
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --work-href=https://www.metacritic.com/tv/shogun-2024 --review-type=all --force
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=game --review-type=all --concurrency=2 --page-size=50 --max-pages=2
```

说明：

- 评论抓取走 `API-first`，不依赖前端 DOM / Nuxt 评论列表解析
- 评论接口域名使用 `https://backend.metacritic.com`
- game 会按 `platform` 拆 scope；movie / tv 不区分平台
- 默认会跳过已经成功抓取过的 `work_href + review_type + platform` scope
- `--force` 会重新抓取已成功 scope
- 每次运行都会写入：
  - `latest_reviews` 当前最新态
  - `review_snapshots` 历史快照
  - `review_fetch_state` scope 状态
  - `crawl_runs` 批次血缘

常用参数：

- `--category=game|movie|tv`
- `--work-href`
- `--limit`
- `--force`
- `--concurrency`
- `--review-type=critic|user|all`
- `--platform`
- `--page-size`
- `--max-pages`
- `--db`
- `--debug`
- `--retries`
- `--proxies`

详见：

- [Phase 4 功能测试](./phase4-reviews-testing.md)

## crawl batch

顺序或并发执行 YAML 批量任务：

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-tasks.yaml
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-concurrent.yaml --concurrency=2
```

说明：

- YAML 使用 `defaults + tasks[]`
- `--concurrency` 会覆盖 `defaults.concurrency`
- 默认遇错继续执行
- 输出包含每个任务的 `run_id` 和最终汇总
- 支持 `kind: list`、`kind: detail`、`kind: reviews`
- 同一个 SQLite 文件在 batch 内会自动串行写入，避免 `SQLITE_BUSY`
- 如果想真正获得并发收益，建议不同任务写入不同 `db`

详见：

- [批量任务](./batch-tasks.md)

## crawl schedule

按 cron 表达式调度批量任务：

```bash
go run ./cmd/metacritic-harvester crawl schedule --file=examples/schedule-jobs.yaml
```

说明：

- 该命令前台常驻运行
- 收到中断信号后停止接收新任务，并等待当前任务收尾
- cron 支持标准 5 段表达式，也支持可选秒字段

详见：

- [调度说明](./scheduling.md)

## latest query

查询 `latest_list_entries`：

```bash
go run ./cmd/metacritic-harvester latest query --db=output/metacritic.db --category=game --metric=metascore --format=table
go run ./cmd/metacritic-harvester latest query --db=output/metacritic.db --work-href=https://www.metacritic.com/game/alpha --format=json
```

## latest export

导出 `latest_list_entries` 当前视图，或通过 `--run-id` 导出单批次 `list_entries` 快照：

```bash
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --category=movie --metric=userscore --format=csv --output=output/movie-userscore.csv
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --format=json --output=output/latest.json
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --run-id=<run-id> --format=json --output=output/run-snapshot.json
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --profile=summary --format=csv --output=output/latest-summary.csv
```

说明：

- `--profile=raw|flat|summary`，默认 `raw`
- `latest export` 的 `raw` 与 `flat` 等价
- `summary` 会输出 `run_id / category / metric / filter_key` 维度的聚合摘要
- `json` 使用 `github.com/bytedance/sonic` 序列化

## latest compare

按两个 `run_id` 对比快照变化：

```bash
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b>
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b> --format=json
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b> --format=csv --include-unchanged
```

对比基于 `list_entries` 快照，而不是 `latest_list_entries` 当前态。

## detail query

查询当前 `work_details` 视图：

```bash
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --category=game
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --work-href=https://www.metacritic.com/game/baldurs-gate-3 --format=json
```

说明：

- `table` 只展示核心标量字段
- `json` 会把 `details_json` 反序列化为结构化 `details`
- 该命令只读打开数据库，不会创建缺失数据库文件

## detail export

导出当前 `work_details` 视图，或通过 `--run-id` 导出单批次 `work_detail_snapshots`：

```bash
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --format=csv --output=output/details.csv
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --category=movie --format=json --output=output/movie-details.json
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --run-id=<detail-run-id> --format=json --output=output/detail-snapshot.json
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --profile=flat --format=csv --output=output/detail-flat.csv
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --run-id=<detail-run-id> --profile=summary --format=json --output=output/detail-summary.json
```

说明：

- `--profile=raw|flat|summary`，默认 `raw`
- `raw` 输出结构化详情；`csv` 会附带原始 `details_json`
- `flat` 会把常用扩展字段扁平化为 `genres_csv / platforms_csv / developers_csv` 等列
- `summary` 会按 `run_id + category` 聚合输出覆盖率摘要

## detail compare

按两个 detail `run_id` 对比详情快照变化：

```bash
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b>
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b> --format=json
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b> --format=csv --include-unchanged
```

说明：

- 对比基于 `work_detail_snapshots`
- 核心标量字段会给出 from/to 值
- 扩展详情字段通过 `details_json_changed`、`from_details_json` 和 `to_details_json` 表达
- Phase 3.5 P2 只补齐读侧，不扩大详情抓取范围

## review query

查询当前 `latest_reviews` 视图：

```bash
go run ./cmd/metacritic-harvester review query --db=output/metacritic.db --category=game --review-type=critic --format=table
go run ./cmd/metacritic-harvester review query --db=output/metacritic.db --work-href=https://www.metacritic.com/movie/boyhood --format=json
```

说明：

- `table` 只展示核心字段
- `json` 会输出完整评论记录
- 支持按 `category / review-type / platform / work-href` 过滤

## review export

导出当前 `latest_reviews` 视图，或通过 `--run-id` 导出单批次 `review_snapshots`：

```bash
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --category=game --review-type=user --format=csv --output=output/game-user-reviews.csv
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --category=tv --format=json --output=output/tv-reviews.json
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --run-id=<review-run-id> --format=json --output=output/review-snapshot.json
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --profile=flat --format=csv --output=output/review-flat.csv
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --profile=summary --format=json --output=output/review-summary.json
```

说明：

- `--profile=raw|flat|summary`，默认 `raw`
- `raw` 会保留 `source_payload_json`
- `flat` 会保留标准化标量列，但去掉原始 payload，更适合 BI / 清洗
- `summary` 会按 `run_id / category / review_type / platform_key` 聚合输出
- 不传 `--run-id` 时读取 `latest_reviews`；传入 `--run-id` 时读取 `review_snapshots`

## review compare

按两个评论抓取 `run_id` 对比快照变化：

```bash
go run ./cmd/metacritic-harvester review compare --db=output/metacritic.db --from-run-id=<review-run-a> --to-run-id=<review-run-b>
go run ./cmd/metacritic-harvester review compare --db=output/metacritic.db --from-run-id=<review-run-a> --to-run-id=<review-run-b> --format=json
go run ./cmd/metacritic-harvester review compare --db=output/metacritic.db --from-run-id=<review-run-a> --to-run-id=<review-run-b> --platform=pc --include-unchanged --format=csv
```

说明：

- 对比基于 `review_snapshots`
- 差异类型包括 `added / removed / changed / unchanged`
- 当前变化判断覆盖 `score / quote / thumbs_up / thumbs_down / version_label / spoiler_flag`

## 数据结构

当前会初始化并实际写入：

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

写入语义：

- `works`：按 `href` 做 `UPSERT`
- `list_entries`：每次抓取都新增快照
- `latest_list_entries`：按 `work_href + category + metric + filter_key` 做 `UPSERT`
- `work_details`：按 `work_href` 做 `UPSERT`，保存详情核心字段和扩展 JSON
- `work_detail_snapshots`：按 `work_href + crawl_run_id` 保存详情历史快照，用于 `detail compare`
- `detail_fetch_state`：记录详情抓取状态、最近成功时间和最近错误
- `latest_reviews`：按 `review_key` 做 `UPSERT`，维护评论最新态
- `review_snapshots`：按 `review_key + crawl_run_id` 保存评论历史快照，用于 `review compare`
- `review_fetch_state`：按 `work_href + review_type + platform_key` 记录评论抓取状态
- `crawl_runs`：记录每次单任务抓取的开始、结束、状态和错误

## 兼容说明

当前版本在原有 Phase 2 基础上引入了更多结构：

- `crawl_runs`
- `list_entries.crawl_run_id`
- `latest_list_entries.source_crawl_run_id`
- `work_details`
- `detail_fetch_state`
- `latest_reviews`
- `review_snapshots`
- `review_fetch_state`

如果是旧库首次升级，程序会自动补齐缺失列；如果你希望结构最干净，建议重新创建数据库再跑一次任务。

## 测试

```bash
go test ./...
```

当前覆盖：

- CLI 参数校验
- URL builder
- parser
- repository
- `httptest` 集成抓取
- detail parser / storage / service / CLI
- review service / CLI / repository
- 批量并发
- schedule 配置与触发
- latest query/export/compare
