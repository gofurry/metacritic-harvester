# Command Examples

## Single-task list crawls

### Game / Metascore

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --pages=1 --db=output/metacritic.db
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --source=auto --pages=1 --db=output/metacritic.db
```

### Game with filters

```bash
go run ./cmd/metacritic-harvester crawl list --category=game --metric=metascore --year=2011:2014 --platform=pc,ps5 --genre=action,rpg --release-type=coming-soon
```

### Movie with filters

```bash
go run ./cmd/metacritic-harvester crawl list --category=movie --metric=userscore --year=2011:2014 --network=netflix,max --genre=drama,thriller --release-type=coming-soon,in-theaters
```

### TV with filters

```bash
go run ./cmd/metacritic-harvester crawl list --category=tv --metric=newest --year=2011:2014 --network=hulu,netflix --genre=drama,thriller
```

## Batch crawls

### Batch into one SQLite database

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-tasks.yaml
```

### Batch with concurrency

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-concurrent.yaml --concurrency=2
```

### Batch into separate SQLite databases

```bash
go run ./cmd/metacritic-harvester crawl batch --file=examples/batch-multi-db.yaml
```

## Scheduling

```bash
go run ./cmd/metacritic-harvester crawl schedule --file=examples/schedule-jobs.yaml
```

## Detail crawls

### Crawl all pending details in one database

```bash
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --source=auto
```

### Crawl only game details

```bash
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=game
```

### Crawl one specific work

```bash
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --work-href=https://www.metacritic.com/game/baldurs-gate-3
```

### Force refresh successful details

```bash
go run ./cmd/metacritic-harvester crawl detail --db=output/metacritic.db --category=tv --force
```

## Latest data

### Query latest rows

```bash
go run ./cmd/metacritic-harvester latest query --db=output/metacritic.db --category=game --metric=metascore
```

### Export latest rows

```bash
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --format=csv --output=output/latest.csv
```

### Export one list snapshot by run id

```bash
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --run-id=<run-id> --format=json --output=output/latest-run.json
```

### Export latest summary rows

```bash
go run ./cmd/metacritic-harvester latest export --db=output/metacritic.db --profile=summary --format=csv --output=output/latest-summary.csv
```

### Compare two runs

```bash
go run ./cmd/metacritic-harvester latest compare --db=output/metacritic.db --from-run-id=<run-a> --to-run-id=<run-b>
```

## Detail read-side

### Query current detail rows

```bash
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --category=game
go run ./cmd/metacritic-harvester detail query --db=output/metacritic.db --work-href=https://www.metacritic.com/game/baldurs-gate-3 --format=json
```

### Export current detail rows

```bash
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --format=csv --output=output/details.csv
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --category=movie --format=json --output=output/movie-details.json
```

### Export one detail snapshot by run id

```bash
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --run-id=<detail-run-id> --format=json --output=output/detail-snapshot.json
```

### Export flattened detail rows

```bash
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --profile=flat --format=csv --output=output/detail-flat.csv
```

### Export detail summary rows

```bash
go run ./cmd/metacritic-harvester detail export --db=output/metacritic.db --run-id=<detail-run-id> --profile=summary --format=json --output=output/detail-summary.json
```

### Compare two detail runs

```bash
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b>
go run ./cmd/metacritic-harvester detail compare --db=output/metacritic.db --from-run-id=<detail-run-a> --to-run-id=<detail-run-b> --format=csv --include-unchanged
```

## Review read-side

### Query current review rows

```bash
go run ./cmd/metacritic-harvester review query --db=output/metacritic.db --category=game --review-type=critic
go run ./cmd/metacritic-harvester review query --db=output/metacritic.db --work-href=https://www.metacritic.com/movie/boyhood --format=json
```

### Export current review rows

```bash
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --category=game --review-type=user --format=csv --output=output/game-user-reviews.csv
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --category=tv --format=json --output=output/tv-reviews.json
```

### Export one review snapshot by run id

```bash
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --run-id=<review-run-id> --format=json --output=output/review-snapshot.json
```

### Export flattened review rows

```bash
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --profile=flat --format=csv --output=output/review-flat.csv
```

### Export review summary rows

```bash
go run ./cmd/metacritic-harvester review export --db=output/metacritic.db --profile=summary --format=json --output=output/review-summary.json
```

### Compare two review runs

```bash
go run ./cmd/metacritic-harvester review compare --db=output/metacritic.db --from-run-id=<review-run-a> --to-run-id=<review-run-b>
go run ./cmd/metacritic-harvester review compare --db=output/metacritic.db --from-run-id=<review-run-a> --to-run-id=<review-run-b> --platform=pc --include-unchanged --format=csv
```
