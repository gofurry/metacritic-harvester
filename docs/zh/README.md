# metacritic-harvester 中文说明

[English README](../../README.md) | [使用方式](./usage.md) | [Serve](./serve.md) | [路线图](./roadmap.md)

![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Release](https://img.shields.io/github/v/release/GoFurry/metacritic-harvester?style=flat&color=blue)
[![Go Report Card](https://goreportcard.com/badge/github.com/GoFurry/metacritic-harvester)](https://goreportcard.com/report/github.com/GoFurry/metacritic-harvester)

一个本地优先的 Go 工具集，用来采集公开的 Metacritic 榜单、详情和评论数据，并写入 SQLite。

## 项目状态

按当前 roadmap 范围来看，项目已经完成到 Phase 6 的第一版，主能力已经闭环。

落到实际上，意味着这些都已经具备：

- 榜单、详情、评论三条抓取链路都已完成
- 当前态视图与不可变快照都已实现
- `latest`、`detail`、`review` 的查询、导出、对比都已实现
- `API-first` 已落地，并在合适的地方保留 HTML / Nuxt fallback
- `batch` 和 `schedule` 已在 CLI 中可用
- 本地 `serve` 运行时与嵌入式控制台已可用

后续重点主要是产品打磨和服务端体验增强，而不是补核心功能。

## 当前能力

- `crawl list`
- `crawl detail`
- `crawl reviews`
- `crawl batch`
- `crawl schedule`
- `serve`
- `latest query / export / compare`
- `detail query / export / compare`
- `review query / export / compare`

存储与历史：

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

运行时行为：

- `crawl list` 和 `crawl detail` 支持 `--source=api|html|auto`
- 默认 source 是 `api`
- `auto` 表示“先 API，失败后回退”
- detail 的 enrich 只在 API 路径没完全覆盖的字段上保留 HTML / Nuxt
- `serve` 支持浏览器直接下载导出
- 即使开启 web 控制台，batch 和 schedule 依然保持 CLI 驱动

## 快速开始

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --source=api --pages=0 --db=output/metacritic.db
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=game --source=api
go run ./cmd/metacritic-harvester crawl reviews --db=output/metacritic.db --category=game --review-type=critic
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --category=game
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db --full-stack --enable-write
```

## Serve 亮点

内置本地服务目前支持：

- 本地 JSON API
- 嵌入式 Vue 控制台
- 基于 SSE 的实时采集日志
- 直接发起 `list / detail / reviews` 任务
- `latest / detail / review` 的浏览器导出下载

当前 serve 边界：

- 默认绑定 `127.0.0.1`
- 默认只读
- 写操作需要显式加 `--enable-write`
- batch 和 schedule 保持在 CLI 侧，web 控制台只说明这部分工作流，不通过 HTTP 直接触发

## 推荐阅读

- [使用方式](./usage.md)
- [Serve 说明](./serve.md)
- [批量任务](./batch-tasks.md)
- [调度说明](./scheduling.md)
- [过滤参数](./filters.md)
- [榜单读侧](./latest.md)
- [路线图](./roadmap.md)

## 工具链

如果需要重新生成数据库访问代码，先安装 `sqlc`：

```bash
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate
```

运行测试：

```bash
go test ./...
```
