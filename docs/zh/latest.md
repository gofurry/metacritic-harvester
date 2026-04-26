# 榜单读侧

`latest` 命令组是只读的，不会触发抓取。

它只聚焦榜单当前视图与榜单快照对比。

## latest query

```bash
go run ./cmd/metacritic-harvester latest query --db=output/metacritic.db --category=game --metric=metascore
go run ./cmd/metacritic-harvester latest query --db=output/metacritic.db --work-href=https://www.metacritic.com/game/alpha/ --format=json
```

支持参数：

- `--db`
- `--category`
- `--metric`
- `--work-href`
- `--filter-key`
- `--limit`
- `--format=table|json`

## latest export

```bash
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --format=csv --output=output/latest.csv
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --run-id=<run-id> --format=json --output=output/run-snapshot.json
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --profile=summary --format=csv --output=output/latest-summary.csv
```

行为：

- 不传 `--run-id` 时读取 `latest_list_entries`
- 传 `--run-id` 时读取 `list_entries`
- 支持 `--profile=raw|flat|summary`
- 对榜单导出来说，`raw` 和 `flat` 等价
- `summary` 会返回按 run 与过滤范围聚合后的数量和名次区间

## latest compare

```bash
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b>
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b> --format=json
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b> --format=csv --include-unchanged
```

行为：

- compare 永远读取 `list_entries`
- 不直接比较 `latest_list_entries`
- `run_id` 是稳定的历史榜单对比单位

## 相关命令

详情当前态和历史对比放在独立的 `detail` 命令组：

```bash
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --category=game
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --format=csv --output=output/details.csv
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b>
```
