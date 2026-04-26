# 过滤参数

这个文档汇总 `crawl list` 当前支持的主要过滤参数。

## 通用年份过滤

所有类别都支持：

```bash
--year=2011:2014
```

内部会映射成 release year 范围。

## 游戏过滤

支持：

- `--platform`
- `--genre`
- `--release-type`
- `--year`

示例：

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --platform=pc,ps5 --genre=action,rpg
```

## 电影过滤

支持：

- `--network`
- `--genre`
- `--release-type`
- `--year`

示例：

```bash
go run ./cmd/metacritic-harvester crawl list --category=movie --metric=userscore --network=netflix,max --genre=drama,thriller
```

## 剧集过滤

支持：

- `--network`
- `--genre`
- `--year`

示例：

```bash
go run ./cmd/metacritic-harvester crawl list --category=tv --metric=newest --network=hulu,netflix --genre=drama,thriller
```

## source 行为

榜单抓取还支持：

```bash
--source=api|html|auto
```

- 默认：`api`
- `html`：强制走旧 HTML 路径
- `auto`：API first，失败后回退 HTML

## 说明

- CLI 多值过滤使用逗号分隔
- finder 映射现在会统一常见别名与命名变体
- 不支持的映射值会返回稳定的 mapping error，不会静默猜测
