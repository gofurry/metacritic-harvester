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

## Phase 5.5：进入 `serve` 之前的补强

状态：P0 / P1 / P2 已完成

目标：

- 在进入 Phase 6 之前，先把当前仓库的运行稳定性、边界安全、可观测性和性能短板补平
- Phase 5.5 不新增新的主抓取范围，也不直接做 UI；主要用于给 `serve` 打工程化基础

当前主要不足：

- `crawl list` / `crawl detail` 在 service 层的 `Source` 零值与 CLI / batch 默认值仍不完全一致，仍存在“对外默认 `api`、对内默认 `html`”的行为分叉
- finder API 对 `platform / network` 的 ID 映射仍是硬编码表；遇到新平台、新流媒体或命名变体时，`API-first` 路径会更脆弱
- `auto` fallback 当前更偏向“整次 run 回退”或“整条 work 回退”，缺少更细粒度的 provenance 和 fallback 诊断信息
- API 抓取路径已有 retry，但缺少全局速率限制、统一 timeout 预算、并发预算、circuit-breaker 一类的运行级保护
- detail enrich 失败目前主要体现在日志 warning；缺少可直接被程序消费的结构化状态，不利于后续 `serve` / console 展示“主详情成功但补充缺失”
- fixture 与回归测试已覆盖主路径，但对真实站点结构漂移、新过滤组合、大样本运行、长时间运行后的行为覆盖还不够系统
- export / compare 已能用，但距离“服务化后长期运行、对外暴露、可运维诊断”的成熟度还有一段距离

P0：

- [x] 收口 `source` 默认语义：CLI、batch、service 统一默认 `api`
- [x] 为 `list / detail / reviews` 增加统一 timeout、速率限制和并发 gate
- [x] 为 `list / detail / review` 的 run 结果补齐 `requested_source / effective_source / fallback_*` 结构化诊断
- [x] 将 detail enrich 收口为结构化结果统计，而不是只留 warning 日志
- [x] 补关键 fallback / source parity / 漂移回归测试

P1：

- [x] 将 finder API 的 `platform / network / genre` 映射收口到集中配置模块，并补标准化与稳定错误
- [x] 细化 fallback 原因：统一使用 `api_request_failed / api_parse_failed / api_mapping_failed / api_missing_required_fields`
- [x] 为 `list / detail / review` 增加 opt-in benchmark / soak 验证入口
- [x] 统一 `query / export / compare` 的读侧错误风格与空结果语义，减少后续 `serve` 层适配成本
- [x] 明确 `auto` 不是双抓取，并在文档中收口 list/detail 的 fallback 粒度差异

P2：

- [x] 预先定义 `serve` 的安全基线：默认仅绑定 `127.0.0.1`；默认只开放健康检查、只读查询和只读导出；抓取触发、batch 执行、schedule 变更、导出落盘等写操作默认关闭，且后续即使开启也仅允许本机访问
- [x] 明确导出文件路径约束：Phase 6 不允许通过 API 写入任意绝对路径；导出落盘仅允许受控目录（建议 `output/exports`），也不允许覆盖 repo 内任意路径
- [x] 将 `crawl_runs`、`detail_fetch_state`、`review_fetch_state` 与现有 export summary 收敛为统一运行观测模型：最近运行列表来自 `crawl_runs`，当前抓取状态来自 `*_fetch_state`，聚合摘要优先复用现有 `summary` 导出统计口径，不新增物化观测表
- [x] 明确 Phase 6 的最小观测实体：`RunStatusView`（运行基本信息）、`RunOutcomeView`（list/detail/review 的结果统计与 fallback/enrich 诊断）、`FetchStateView`（detail/review 的最近状态与错误信息），优先在 repository/service 层做只读聚合
- [x] 评估 SQLite 服务化边界并形成结论：Phase 6 继续使用 SQLite，不提前抽象成更重后端；默认保持 WAL 模式并保留显式 checkpoint 能力；同一 DB 的写任务仍应串行或受严格 gate 控制；读多写少是前提，若后续出现高频并发写入，再进入新阶段评估存储升级
- [x] 明确 Phase 6 前的非功能性验证重点：观测 WAL 增长、长读查询与写任务并发下的 `SQLITE_BUSY` 风险、checkpoint 后 WAL 回收效果
- [x] 评估当前抽象边界：storage 已足够支撑 `serve` 的只读/读写分离；`list/detail/review` 的 app service 可直接复用为任务执行层；CLI 格式化层不直接搬进 HTTP，HTTP 只复用 repository/service；当前 `source adapter` 先不重构，除非后续扩展到更多站点才再评估

## Phase 6：服务化

状态：进行中

目标：

- 保留 CLI 的同时新增 `serve`
- 复用现有 `app + storage + source`
- 提供 HTTP 查询和任务触发入口
- `serve` 默认以纯后端模式启动，仅暴露 API、健康检查和任务触发接口
- 通过参数可开启全栈模式；全栈模式会额外提供一个 embed 到二进制的控制台前端页面
- 前端控制台用于查看运行状态、触发任务、浏览结果和实时观察采集日志进度

当前进展：

- [x] 新增 `serve` 命令，并接入根 CLI
- [x] 提供基础 HTTP API：健康检查、配置、运行列表、任务列表、日志、latest/detail/review 查询、detail/review fetch state
- [x] 提供本机写入保护下的任务触发接口：`list / detail / reviews`
- [x] 接入任务管理器，复用现有 `app service` 执行抓取任务
- [x] 提供实时日志流接口：`/api/logs/stream`
- [x] 提供嵌入式全栈前端控制台，可查看任务、运行、查询结果和实时日志
- [ ] 继续补服务端导出、批量/调度控制与更完整的运维面板

## 建议优先级

下一步更推荐按这个顺序推进：

1. Phase 6：补齐 `serve` 的剩余控制面与导出能力
2. 后续服务化增强：更细的运维、鉴权和存储演进

原因：

- 当前抓取与读侧主链路、运行护栏和观测基线已经具备，Phase 6 可以直接围绕服务边界、接口设计和控制台交互推进
- 现阶段最值得继续投入的是把 `serve` 的操作面、结果面和导出面补完整，而不是再回头补基础设施
- SQLite、fallback、速率控制和本机安全边界已经前置收口，可以作为后续服务化迭代的稳定底座
