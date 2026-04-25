# Roadmap

## 当前状态

当前仓库已经完成：

- Phase 1：CLI 列表抓取、SQLite 落库、`sqlc`
- Phase 2：过滤参数、`latest_list_entries`、批量任务、并发批量、调度、latest 读侧能力
- Phase 2.5：稳定性与数据一致性补强
- Phase 3：详情页抓取
- Phase 3.5：详情体系补强与 Nuxt `where_to_buy / where_to_watch`

当前核心语义是：

- `works`：按 `href` 更新作品主实体
- `list_entries`：每次列表抓取都新增快照
- `latest_list_entries`：维护当前最新榜单视图
- `crawl_runs`：为每次抓取生成 `run_id`
- `work_details`：维护当前详情最新视图
- `work_detail_snapshots`：维护详情历史快照
- `detail_fetch_state`：维护详情抓取状态
- `review_fetch_state`：为评论抓取预留状态管理

这套模型已经能支持：

- 历史分数变化分析
- 当前最新榜单查询
- 两次抓取按 `run_id` 做差异对比
- 基于已有榜单结果继续抓取详情
- 从 Nuxt `#__NUXT_DATA__` 提取 `where_to_buy / where_to_watch`

## Phase 1

状态：已完成

- `cobra` CLI
- `crawl list`
- `game / movie / tv`
- `metascore / userscore / newest`
- 统一 URL builder
- SQLite + `modernc.org/sqlite`
- `sqlc` repository
- 代理与重试
- 测试闭环

## Phase 2

状态：已完成

已完成内容：

- `crawl list` Filter
- `year / platform / network / genre / release-type`
- `latest_list_entries`
- 快照表 + 最新表双写
- `crawl batch --file=...`
- 批量任务 YAML `defaults + tasks[]`
- 批量任务并发 `--concurrency`
- `crawl schedule --file=...`
- `crawl_runs` 与 `run_id`
- `latest query`
- `latest export`
- `latest compare`
- JSON 导出使用 `sonic`
- 调度使用 `robfig/cron`

兼容说明：

- 旧库升级时程序会自动补齐新增列
- 如果希望结构最干净，建议重建数据库

## Phase 2.5：稳定性与数据一致性补强

状态：已完成

完成内容：

- 抓取失败、解析失败、写库失败会正确传播到 CLI / batch / schedule，并将 `crawl_runs` 标记为失败
- 列表快照按页事务写入，避免 `works`、`list_entries`、`latest_list_entries` 出现部分提交
- 调度任务防止同一 job 重叠执行，并继承外层 context，支持更可靠的中断退出
- `latest query/export/compare` 与 `detail query/export/compare` 使用只读数据库打开路径
- 支持用户显式传入 `--checkpoint`，在读命令结束后执行 `wal_checkpoint(TRUNCATE)` 清理 WAL 副文件
- `latest compare` 增加复合索引，历史快照增长后仍有明确查询支撑
- `RetryTracker` 增加并发保护，为后续提升 collector 并发预留安全边界
- 旧库升级会用 `legacy-upgrade-v1` 回填历史空血缘，避免 `run_id` 断层继续扩大
- 抓取结果新增 `PagesScheduled / PagesSucceeded / PagesWritten`，`PagesVisited` 保持兼容并表示成功响应页数

说明：

- Phase 2.5 不新增抓取范围，主要用于完成 Phase 2 到 Phase 3 之间的数据可信度和运行稳定性补强

## Phase 3：详情页抓取

状态：已完成

完成内容：

- 新增 `crawl detail`，从已有 `works.href` 抓取 game / movie / tv 详情页
- 新增 `work_details`，采用核心字段 + 类别扩展 JSON 的轻量详情模型
- 详情抓取会更新 `detail_fetch_state`，默认跳过已成功抓取的作品，`--force` 可强制刷新
- 详情抓取会补强 `works` 的名称与发售日期等基础字段

## Phase 3.5：详情体系补强

状态：已完成

完成内容：

- 详情抓取补齐 `crawl_runs` 血缘、失败分类、恢复语义与更明确的进度日志
- 新增 `work_detail_snapshots`，形成“最新态 + 历史快照”双写模型，支持按 `run_id` 回溯与详情变更对比
- `crawl detail` 接入 batch / schedule，并支持可控并发、run 级 collector 复用和更稳定的 SQLite 写入策略
- 新增独立顶层 `detail query / export / compare`
- 从详情页 Nuxt `#__NUXT_DATA__` 中提取：
  - `game`：`where_to_buy`
  - `movie / tv`：`where_to_watch`
- 上述 buy/watch 结果先进入 `work_details.details_json` 与 `work_detail_snapshots.details_json`

## Phase 4：评论抓取

状态：已完成

已完成内容：

- 新增 `crawl reviews`
- 新增顶层 `review query / export / compare`
- 评论抓取改为 `API-first`，接口域名为 `https://backend.metacritic.com`
- 使用两层接口：
  - `composer` 页面接口拿上下文、summary、game 平台列表
  - `reviews/metacritic/...` 列表接口拿真实评论分页数据
- game / movie / tv 均已接入评论抓取
- 支持 `critic` 与 `user` 两类评论，以及 game 平台 scope
- 支持 `offset / limit` 分页，以及 `sentiment`、排序等评论参数
- 支持 `latest_reviews + review_snapshots` 双写
- 支持 `review_fetch_state` scope 状态管理与恢复
- 支持 `crawl_runs` 与 `run_id` 血缘，以及 batch / schedule 接入 reviews 任务

当前落库语义：

- `latest_reviews`：按 `review_key` 做 `UPSERT`，维护评论最新视图
- `review_snapshots`：按 `review_key + crawl_run_id` 保存历史快照
- `review_fetch_state`：按 `work_href + review_type + platform_key` 维护 scope 状态
- `crawl_runs`：为每次 `crawl reviews` 生成批次血缘

补强完成内容：

- 评论 API fixture 与样本测试已补强
- `critic` / `user` 评论字段标准化规则已收口
- 明确维持 scope 级恢复，不实现页级 checkpoint

说明：

- Phase 4 明确采用“后端接口优先”策略，因为评论天然具有类型、平台、分页和来源字段，直接解析前端页面会更脆弱、也更难做增量抓取
- 样本已经表明 `composer` 页适合做“上下文 + summary + 平台枚举”，而不是直接当评论列表数据源
- `review_fetch_state` 继续维持 scope 级恢复；评论列表属于 append-like 分页流，offset 会随新评论写入漂移，而 `latest_reviews + review_snapshots` 已能保证 scope 重抓幂等
- 更丰富的 review 导出与分析视图转入 Phase 5 推进

## Phase 5：更完整的导出与分析

状态：已完成

已完成：

- `latest / detail / review export` 已统一支持当前态导出与 `--run-id` 快照导出
- 三条导出链路已统一支持 `--profile=raw|flat|summary`
- `latest export` 已补齐按批次/范围聚合的摘要导出
- `detail export` 已补齐扁平化导出与按批次/类别聚合的摘要导出
- `review export` 已补齐当前态、快照、扁平化与摘要导出
- review 读侧输出目标已收口为：
  - Row export：一行一条评论，基于标准化字段输出
  - Compare export：一行一条评论差异，基于 `review compare` 差异列输出

说明：

- Phase 5 只增强读侧与导出，不改抓取 schema 和抓取流程
- `query` 继续专注当前态浏览；批次导出统一由 `export --run-id` 承担
- `latest` 的 `raw / flat` 等价；`detail` 与 `review` 的 `flat` 更适合 BI / 清洗
- `source_payload_json` 继续保留在 review `raw` 导出里，作为异构字段兜底

## Phase 5-1：列表与详情切换到后端 API 优先

状态：已完成

已完成内容：

- `crawl list` 新增 finder API adapter，并接入 `game / movie / tv`、`metascore / userscore / newest`、分页与主要过滤参数映射
- `crawl detail` 新增 composer API adapter，并覆盖 `game / movie / tv` 的核心详情字段
- `crawl list` / `crawl detail` 新增统一 `--source=api|html|auto`，CLI 与 batch 默认值均为 `api`
- `auto` 语义已落地为“先 API，失败后回退 HTML”
- `detail` 在 `api` 路径下已实现 `API + enrich`：API 提供主详情，HTML / Nuxt 仅补 `where_to_buy`、`where_to_watch` 等当前仍需要的补充字段

测试与验证：

- 已将 list/detail 样本转为 `internal/source/metacritic/api/testdata` 下的 fixture
- 已补 finder / composer 的 API 解析测试，覆盖 `game / movie / tv`
- 已补 list/detail 的 source 路径测试，覆盖：
  - `source=api`
  - `source=auto` 下 API 失败回退 HTML
  - detail API 成功但 enrich 失败时仍视为成功
- 已补 source 参数的 CLI / config / batch 测试
- 已增加轻量 benchmark，作为 API 与 HTML 路径的趋势基线

说明：

- 当前 `docs/sample/list-sample.txt` 与 `docs/sample/details-sample.txt` 已足够支撑本阶段实现；额外样本转为后续补边界使用
- Phase 5-1 的目标不是移除 HTML / Nuxt，而是把默认抓取主链路切换为更稳定的 `API-first`
- 本阶段未改数据库 schema，也未改 current/latest/snapshot 写入语义

## Phase 6：服务化

状态：未开始

目标：

- 保留 CLI 的同时新增 `serve`
- 复用现有 `app + storage + source`
- 提供 HTTP 查询和任务触发入口
- `serve` 默认以纯后端模式启动，仅暴露 API、健康检查和任务触发接口
- 通过参数可开启全栈模式；全栈模式会额外提供一个 embed 到二进制的简易控制台前端页面
- 前端控制台保持轻量，用于查看运行状态、触发任务和浏览基础结果

## 建议优先级

下一步更推荐按这个顺序推进：

1. Phase 4：`crawl reviews`
2. Phase 5-1：list / detail 切到 `API-first`
3. Phase 5：更丰富的导出与分析
4. Phase 6：`serve`

原因：

- 评论抓取是当前数据面最自然的下一层，而且它本身就更适合直接走后端接口
- 完成评论后，再把 list / detail 统一切到 `API-first`，可以把三条主抓取链路收敛到同一套接口层
- 更丰富的导出与服务化会继续受益于统一的 source adapter 与更稳定的结构化数据输入
