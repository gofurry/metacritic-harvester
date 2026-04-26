# Serve

`serve` 会在现有 repository、storage 和 app service 之上启动一个本地 HTTP 运行时。

它支持两种模式：

- 纯后端
- 带嵌入式 Vue 控制台的全栈模式

## 快速开始

纯后端：

```bash
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db
```

全栈控制台：

```bash
go run ./cmd/metacritic-harvester serve --db=output/metacritic.db --full-stack --enable-write
```

默认地址：

```text
127.0.0.1:36666
```

## Flags

- `--addr`
- `--db`
- `--full-stack`
- `--enable-write`
- `--base-url`
- `--backend-base-url`

## 安全基线

- 默认只绑定 loopback
- 默认只读
- 写接口需要 `--enable-write`
- 默认基线下，写请求仍然只允许本机访问

详见：[Serve 基线](./serve-baseline.md)

## 只读接口

- `GET /healthz`
- `GET /api/config`
- `GET /api/runs`
- `GET /api/overview`
- `GET /api/tasks`
- `GET /api/tasks/{id}`
- `GET /api/logs`
- `GET /api/logs/stream`
- `GET /api/latest`
- `GET /api/detail`
- `GET /api/review`
- `GET /api/export/latest`
- `GET /api/export/detail`
- `GET /api/export/review`
- `GET /api/detail/state`
- `GET /api/review/state`

## 写接口

- `POST /api/tasks/list`
- `POST /api/tasks/detail`
- `POST /api/tasks/reviews`

## 导出下载

服务端支持这三类导出的浏览器下载：

- `latest`
- `detail`
- `review`

查询参数和 CLI export 基本保持一致：

- `format=csv|json`
- `profile=raw|flat|summary`
- `run_id` 用于快照导出
- 每种导出原本就支持的 category/work/platform/filter 过滤参数

下载会直接流式返回到客户端，不会在服务端落盘。

## 当前 Web 控制台范围

嵌入式控制台当前主要聚焦：

- 发起 `list / detail / reviews`
- 查看最近 runs
- 查看当前进程内 tasks
- 下载导出
- 查看实时采集日志

batch 和 schedule 仍然保留在 CLI；控制台只负责说明这个工作流，不直接通过 HTTP 触发。
