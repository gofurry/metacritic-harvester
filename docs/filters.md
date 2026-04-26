# Filters

This file summarizes the main filter flags currently supported by `crawl list`.

## Shared filter

All categories support:

```bash
--year=2011:2014
```

This maps internally to a release year range.

## Game filters

Supported flags:

- `--platform`
- `--genre`
- `--release-type`
- `--year`

Example:

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --platform=pc,ps5 --genre=action,rpg
```

## Movie filters

Supported flags:

- `--network`
- `--genre`
- `--release-type`
- `--year`

Example:

```bash
go run ./cmd/metacritic-harvester crawl list --category=movie --metric=userscore --network=netflix,max --genre=drama,thriller
```

## TV filters

Supported flags:

- `--network`
- `--genre`
- `--year`

Example:

```bash
go run ./cmd/metacritic-harvester crawl list --category=tv --metric=newest --network=hulu,netflix --genre=drama,thriller
```

## Source behavior

List crawling also supports:

```bash
--source=api|html|auto
```

- default: `api`
- `html`: force the legacy HTML path
- `auto`: API first, fallback to HTML on failure

## Notes

- CLI multi-value filters use comma-separated values
- finder mapping now normalizes common aliases and naming variants
- unsupported mapping values return stable mapping errors instead of silently guessing
