# 路线图

## 当前状态

按当前本地优先范围来看，项目已经可以视为在 Phase 6 首版范围内功能完备。

当前主链路包括：

- 榜单、详情、评论抓取
- 当前态视图与不可变快照
- `latest / detail / review` 的查询、导出、对比
- `API-first` 抓取与受控 fallback
- CLI 中的 batch 和 schedule
- 本地 `serve` 运行时与嵌入式控制台

剩余工作主要是服务端体验和边界增强，而不是缺少主功能。

## 数据模型状态

当前核心表：

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

已经足够支持：

- 当前态查询
- 历史快照回溯
- 按 `run_id` 对比
- 导出与摘要视图

## Phase 1

状态：已完成

交付：

- Cobra CLI 基础框架
- SQLite + `sqlc`
- `crawl list`
- `game / movie / tv`
- `metascore / userscore / newest`

## Phase 2

状态：已完成

交付：

- 列表过滤参数
- `latest_list_entries`
- `crawl batch`
- `crawl schedule`
- `crawl_runs`
- `latest query / export / compare`

## Phase 2.5

状态：已完成

交付：

- 稳定性和一致性补强
- 列表事务写入
- WAL / checkpoint 支持
- 更稳妥的 batch / schedule 语义

## Phase 3

状态：已完成

交付：

- `crawl detail`
- `work_details`
- `detail_fetch_state`
- game / movie / tv 详情抓取

## Phase 3.5

状态：已完成

交付：

- `work_detail_snapshots`
- detail run 血缘、恢复、诊断
- `detail query / export / compare`
- detail 接入 batch / schedule
- `where_to_buy / where_to_watch` 抽取

## Phase 4

状态：已完成

交付：

- `crawl reviews`
- `review query / export / compare`
- 评论切换为 `API-first`
- `latest_reviews + review_snapshots`
- `review_fetch_state`
- scope 级恢复与字段标准化

## Phase 5

状态：已完成

交付：

- `--run-id` 快照导出
- `--profile=raw|flat|summary`
- 更适合分析和 BI 的导出形态

## Phase 5-1

状态：已完成

交付：

- `crawl list --source=api|html|auto`
- `crawl detail --source=api|html|auto`
- list finder API adapter
- detail composer API adapter
- `API-first` 主路径
- `auto` fallback 和 detail enrich 规则

## Phase 5.5

状态：已完成

交付：

- source 默认语义统一
- 统一运行保护
- 结构化 fallback / enrich 诊断
- finder 映射集中化
- opt-in benchmark / soak 入口
- `serve` 前的安全、观测和 SQLite 边界结论

## Phase 6

状态：进行中，首版已完成

交付：

- `serve`
- 本地 HTTP API
- 嵌入式控制台
- 实时采集日志
- 本地发起 list / detail / reviews
- 浏览器下载导出

当前边界：

- 默认 loopback-only
- 默认只读
- 写操作需要显式开启
- batch 和 schedule 仍保留在 CLI

## 下一步建议

1. 继续打磨 Phase 6 的服务端体验
2. 只有在真实使用证明必要时，再评估更重的鉴权、观测或存储演进
