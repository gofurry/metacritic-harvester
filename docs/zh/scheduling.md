# 调度说明

`crawl schedule` 会以前台方式启动一个本地调度器，按 cron 表达式触发 batch 文件。

## 命令

```bash
go run ./cmd/metacritic-harvester crawl schedule --file=examples/schedule-jobs.yaml
```

## 调度文件结构

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

## 支持字段

顶层：

- `timezone`
- `jobs`

每个 job：

- `name`
- `cron`
- `batch_file`
- `enabled`
- `concurrency`

## 运行行为

- job 触发时会重新读取目标 batch YAML
- 底层每个 crawl 任务仍然会生成自己的 `run_id`
- scheduler 是本地、前台、文件驱动的
- 收到中断后会停止接收新 job，并等待在途任务收尾

## 说明

- schedule 仍然是 CLI-only
- 多个 job 写同一个 SQLite 文件时，写侧仍会受 gate 控制
- 建议先用小任务验证，再挂长期调度
