package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadBatchFileAndBuildConfigs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yaml")
	content := `
defaults:
  db: output/default.db
  pages: 3
  retries: 2
  concurrency: 4
  debug: true
  proxies:
    - http://127.0.0.1:7897
tasks:
  - name: game-task
    category: game
    metric: metascore
    source: auto
    year: "2011:2014"
    platform: [pc, ps5]
    genre: [action, rpg]
    release-type: [coming-soon]
  - kind: detail
    category: movie
    work-href: /movie/test-film
    db: output/movie.db
    force: true
    limit: 5
    detail-concurrency: 3
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	file, err := LoadBatchFile(path)
	if err != nil {
		t.Fatalf("LoadBatchFile() error = %v", err)
	}

	configs, err := BuildBatchTaskConfigs(file)
	if err != nil {
		t.Fatalf("BuildBatchTaskConfigs() error = %v", err)
	}
	runConfig, err := BuildBatchRunConfig(file, 0)
	if err != nil {
		t.Fatalf("BuildBatchRunConfig() error = %v", err)
	}

	if len(configs) != 2 {
		t.Fatalf("expected 2 task configs, got %d", len(configs))
	}
	if runConfig.Concurrency != 4 {
		t.Fatalf("expected inherited concurrency 4, got %d", runConfig.Concurrency)
	}
	if configs[0].Kind != BatchTaskKindList || configs[1].Kind != BatchTaskKindDetail {
		t.Fatalf("unexpected task kinds: %+v", configs)
	}
	if configs[0].Name != "game-task" {
		t.Fatalf("expected explicit task name, got %q", configs[0].Name)
	}
	if configs[1].Name != "detail-single-2" {
		t.Fatalf("expected generated detail task name, got %q", configs[1].Name)
	}
	if configs[0].List == nil || configs[0].List.DBPath != filepath.Clean("output/default.db") {
		t.Fatalf("unexpected inherited list db path: %+v", configs[0].List)
	}
	if configs[0].List.Source != CrawlSourceAuto {
		t.Fatalf("expected list source auto, got %q", configs[0].List.Source)
	}
	if configs[1].Detail == nil || configs[1].Detail.DBPath != filepath.Clean("output/movie.db") {
		t.Fatalf("unexpected detail db path: %+v", configs[1].Detail)
	}
	if configs[1].Detail.Source != CrawlSourceAPI {
		t.Fatalf("expected default detail source api, got %q", configs[1].Detail.Source)
	}
	if configs[0].List.Task.MaxPages != 3 {
		t.Fatalf("unexpected list page defaults: %+v", configs[0].List.Task)
	}
	if configs[1].Detail.Task.Limit != 5 || !configs[1].Detail.Task.Force || configs[1].Detail.Task.Concurrency != 3 {
		t.Fatalf("unexpected detail task parsing: %+v", configs[1].Detail.Task)
	}
	if configs[1].Detail.Task.WorkHref != "https://www.metacritic.com/movie/test-film" {
		t.Fatalf("unexpected detail work href: %q", configs[1].Detail.Task.WorkHref)
	}
}

func TestBuildBatchTaskConfigsRejectsSourceForReviews(t *testing.T) {
	t.Parallel()

	file := BatchFile{
		Tasks: []BatchTaskSpec{
			{
				Kind:       "reviews",
				Category:   "movie",
				ReviewType: "critic",
				Source:     "api",
			},
		},
	}

	if _, err := BuildBatchTaskConfigs(file); err == nil {
		t.Fatal("expected reviews source validation error")
	}
}

func TestBuildBatchRunConfigCLIOverrideWins(t *testing.T) {
	t.Parallel()

	file := BatchFile{
		Defaults: BatchDefaults{
			Concurrency: intPtr(2),
		},
		Tasks: []BatchTaskSpec{
			{
				Category: "game",
				Metric:   "metascore",
			},
		},
	}

	runConfig, err := BuildBatchRunConfig(file, 5)
	if err != nil {
		t.Fatalf("BuildBatchRunConfig() error = %v", err)
	}
	if runConfig.Concurrency != 5 {
		t.Fatalf("expected override concurrency 5, got %d", runConfig.Concurrency)
	}
}

func TestLoadBatchFileRejectsEmptyTasks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yaml")
	if err := os.WriteFile(path, []byte("defaults:\n  pages: 1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := LoadBatchFile(path); err == nil {
		t.Fatal("expected error for empty tasks")
	}
}

func TestBuildBatchTaskConfigsRejectsInvalidListTask(t *testing.T) {
	t.Parallel()

	file := BatchFile{
		Defaults: BatchDefaults{
			DBPath:  "output/default.db",
			Pages:   intPtr(1),
			Retries: intPtr(1),
		},
		Tasks: []BatchTaskSpec{
			{
				Category: "tv",
				Metric:   "newest",
				Platform: []string{"pc"},
			},
		},
	}

	if _, err := BuildBatchTaskConfigs(file); err == nil {
		t.Fatal("expected invalid task error")
	}
}

func TestBuildBatchTaskConfigsRejectsListOnlyFieldsForDetail(t *testing.T) {
	t.Parallel()

	file := BatchFile{
		Tasks: []BatchTaskSpec{
			{
				Kind:     "detail",
				Category: "game",
				Metric:   "metascore",
			},
		},
	}

	if _, err := BuildBatchTaskConfigs(file); err == nil {
		t.Fatal("expected detail validation error")
	}
}

func TestBuildBatchTaskConfigsAppliesDefaultDetailConcurrency(t *testing.T) {
	t.Parallel()

	file := BatchFile{
		Defaults: BatchDefaults{
			DBPath: "output/default.db",
		},
		Tasks: []BatchTaskSpec{
			{
				Kind:     "detail",
				Category: "tv",
			},
		},
	}

	configs, err := BuildBatchTaskConfigs(file)
	if err != nil {
		t.Fatalf("BuildBatchTaskConfigs() error = %v", err)
	}
	if len(configs) != 1 || configs[0].Detail == nil {
		t.Fatalf("unexpected configs: %+v", configs)
	}
	if configs[0].Detail.Task.Concurrency != 1 || configs[0].Detail.Concurrency != 1 {
		t.Fatalf("expected default detail concurrency 1, got %+v", configs[0].Detail)
	}
}

func TestBuildBatchTaskConfigsParsesReviewSentimentAndSort(t *testing.T) {
	t.Parallel()

	file := BatchFile{
		Defaults: BatchDefaults{
			DBPath: "output/default.db",
		},
		Tasks: []BatchTaskSpec{
			{
				Kind:              "reviews",
				Category:          "game",
				ReviewType:        "critic",
				Sentiment:         "negative",
				Sort:              "publication",
				ReviewConcurrency: intPtr(2),
				PageSize:          intPtr(25),
				MaxPages:          intPtr(4),
			},
		},
	}

	configs, err := BuildBatchTaskConfigs(file)
	if err != nil {
		t.Fatalf("BuildBatchTaskConfigs() error = %v", err)
	}
	if len(configs) != 1 || configs[0].Review == nil {
		t.Fatalf("unexpected configs: %+v", configs)
	}
	if configs[0].Review.Task.Sentiment != "negative" || configs[0].Review.Task.Sort != "publication" {
		t.Fatalf("unexpected review task fields: %+v", configs[0].Review.Task)
	}
}

func intPtr(v int) *int {
	return &v
}
