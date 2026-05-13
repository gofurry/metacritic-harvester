package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func TestLatestQueryCommand(t *testing.T) {
	t.Parallel()

	dbPath := seedLatestTestDB(t)

	cmd := newLatestQueryCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath, "--category=game", "--metric=metascore"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "SOURCE_RUN_ID") || !strings.Contains(output, "run-2") {
		t.Fatalf("expected latest query output to include run id, got %q", output)
	}
}

func TestLatestQueryCommandWithCheckpoint(t *testing.T) {
	t.Parallel()

	dbPath := seedLatestTestDB(t)

	cmd := newLatestQueryCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath, "--category=game", "--metric=metascore", "--checkpoint"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if output := out.String(); !strings.Contains(output, "SOURCE_RUN_ID") {
		t.Fatalf("expected latest query output to include header, got %q", output)
	}
}

func TestLatestQueryCommandRequiresExistingDB(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "missing", "latest.db")
	cmd := newLatestQueryCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath})

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected Execute() to fail for missing database")
	}
	if output := out.String(); !strings.Contains(output, "latest query failed") || !strings.Contains(output, "db=") {
		t.Fatalf("expected wrapped latest query error, got %q", output)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("expected database file not to be created, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Dir(dbPath)); !os.IsNotExist(err) {
		t.Fatalf("expected database directory not to be created, stat err=%v", err)
	}
}

func TestLatestExportCommandJSON(t *testing.T) {
	t.Parallel()

	dbPath := seedLatestTestDB(t)
	outputPath := filepath.Join(t.TempDir(), "latest.json")

	cmd := newLatestExportCommand()
	cmd.SetArgs([]string{"--db", dbPath, "--format=json", "--output", outputPath})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(content), "\"source_crawl_run_id\":\"run-2\"") {
		t.Fatalf("expected exported json to include latest run id, got %q", string(content))
	}
}

func TestLatestExportCommandRunIDAndSummaryProfile(t *testing.T) {
	t.Parallel()

	dbPath := seedLatestTestDB(t)

	rawPath := filepath.Join(t.TempDir(), "latest-run.json")
	rawCmd := newLatestExportCommand()
	rawCmd.SetArgs([]string{"--db", dbPath, "--run-id=run-1", "--format=json", "--output", rawPath})
	if err := rawCmd.Execute(); err != nil {
		t.Fatalf("raw Execute() error = %v", err)
	}
	rawContent, err := os.ReadFile(rawPath)
	if err != nil {
		t.Fatalf("ReadFile(raw) error = %v", err)
	}
	rawOutput := string(rawContent)
	if !strings.Contains(rawOutput, "\"source_crawl_run_id\":\"run-1\"") || !strings.Contains(rawOutput, "\"metascore\":\"91\"") {
		t.Fatalf("expected run snapshot export to include run-1 payload, got %q", rawOutput)
	}

	summaryPath := filepath.Join(t.TempDir(), "latest-summary.csv")
	summaryCmd := newLatestExportCommand()
	summaryCmd.SetArgs([]string{"--db", dbPath, "--profile=summary", "--format=csv", "--output", summaryPath})
	if err := summaryCmd.Execute(); err != nil {
		t.Fatalf("summary Execute() error = %v", err)
	}
	summaryContent, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("ReadFile(summary) error = %v", err)
	}
	summaryOutput := string(summaryContent)
	if !strings.Contains(summaryOutput, "distinct_work_count") || !strings.Contains(summaryOutput, "run-2") {
		t.Fatalf("expected summary export to include run id and aggregate headers, got %q", summaryOutput)
	}
}

func TestLatestCompareCommandCSV(t *testing.T) {
	t.Parallel()

	dbPath := seedLatestTestDB(t)

	cmd := newLatestCompareCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath, "--from-run-id=run-1", "--to-run-id=run-2", "--format=csv"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "change_type") || !strings.Contains(output, "changed") {
		t.Fatalf("expected compare csv output, got %q", output)
	}
}

func seedLatestTestDB(t *testing.T) string {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "latest.db")
	db, err := storage.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	work := domain.Work{
		Name:        "Alpha",
		Href:        "https://www.metacritic.com/game/alpha",
		ImageURL:    "https://www.metacritic.com/images/alpha.jpg",
		ReleaseDate: "Apr 23, 2026",
		Category:    domain.CategoryGame,
	}
	if err := repo.UpsertWork(ctx, work); err != nil {
		t.Fatalf("UpsertWork() error = %v", err)
	}

	task := domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore}
	if err := repo.CreateCrawlRun(ctx, "run-1", "crawl list", "task-1", task, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun(run-1) error = %v", err)
	}
	if err := repo.CreateCrawlRun(ctx, "run-2", "crawl list", "task-2", task, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun(run-2) error = %v", err)
	}

	first := domain.ListEntry{
		CrawlRunID: "run-1",
		WorkHref:   work.Href,
		Category:   domain.CategoryGame,
		Metric:     domain.MetricMetascore,
		Page:       1,
		Rank:       1,
		Metascore:  "91",
		UserScore:  "8.4",
		FilterKey:  task.Filter.Key(),
		CrawledAt:  time.Now().UTC(),
	}
	second := first
	second.CrawlRunID = "run-2"
	second.Rank = 2
	second.Metascore = "93"
	second.CrawledAt = time.Now().UTC().Add(time.Minute)

	if err := repo.InsertListEntry(ctx, first); err != nil {
		t.Fatalf("InsertListEntry(first) error = %v", err)
	}
	if err := repo.InsertListEntry(ctx, second); err != nil {
		t.Fatalf("InsertListEntry(second) error = %v", err)
	}
	if err := repo.UpsertLatestListEntry(ctx, second); err != nil {
		t.Fatalf("UpsertLatestListEntry() error = %v", err)
	}

	return dbPath
}
