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

func TestDetailQueryCommandTableAndJSON(t *testing.T) {
	t.Parallel()

	dbPath := seedDetailReadTestDB(t)

	tableCmd := newDetailQueryCommand()
	var tableOut bytes.Buffer
	tableCmd.SetOut(&tableOut)
	tableCmd.SetErr(&tableOut)
	tableCmd.SetArgs([]string{"--db", dbPath, "--category=game"})
	if err := tableCmd.Execute(); err != nil {
		t.Fatalf("table Execute() error = %v", err)
	}
	if output := tableOut.String(); !strings.Contains(output, "WORK_HREF") || !strings.Contains(output, "Alpha Prime") {
		t.Fatalf("expected table output to include header and title, got %q", output)
	}

	jsonCmd := newDetailQueryCommand()
	var jsonOut bytes.Buffer
	jsonCmd.SetOut(&jsonOut)
	jsonCmd.SetErr(&jsonOut)
	jsonCmd.SetArgs([]string{"--db", dbPath, "--work-href=https://www.metacritic.com/game/alpha", "--format=json"})
	if err := jsonCmd.Execute(); err != nil {
		t.Fatalf("json Execute() error = %v", err)
	}
	output := jsonOut.String()
	if strings.TrimSpace(output) == "[]" || !strings.Contains(output, "\"details\"") || !strings.Contains(output, "\"platforms\":[\"PC\"]") || !strings.Contains(output, "\"where_to_buy\"") {
		t.Fatalf("expected json output to include parsed details, got %q", output)
	}
}

func TestDetailQueryCommandWithCheckpoint(t *testing.T) {
	t.Parallel()

	dbPath := seedDetailReadTestDB(t)

	cmd := newDetailQueryCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath, "--category=game", "--format=json", "--checkpoint"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if output := out.String(); !strings.Contains(output, "\"details\"") {
		t.Fatalf("expected json output, got %q", output)
	}
}

func TestDetailQueryCommandRequiresExistingDB(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "missing", "detail.db")
	cmd := newDetailQueryCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath})

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected Execute() to fail for missing database")
	}
	if output := out.String(); !strings.Contains(output, "detail query failed") || !strings.Contains(output, "db=") {
		t.Fatalf("expected wrapped detail query error, got %q", output)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("expected database file not to be created, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Dir(dbPath)); !os.IsNotExist(err) {
		t.Fatalf("expected database directory not to be created, stat err=%v", err)
	}
}

func TestDetailExportCommandCSVAndJSON(t *testing.T) {
	t.Parallel()

	dbPath := seedDetailReadTestDB(t)

	csvPath := filepath.Join(t.TempDir(), "details.csv")
	csvCmd := newDetailExportCommand()
	csvCmd.SetArgs([]string{"--db", dbPath, "--format=csv", "--output", csvPath})
	if err := csvCmd.Execute(); err != nil {
		t.Fatalf("csv Execute() error = %v", err)
	}
	csvContent, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("ReadFile(csv) error = %v", err)
	}
	if content := string(csvContent); !strings.Contains(content, "details_json") || !strings.Contains(content, "platforms") || !strings.Contains(content, "RPG") {
		t.Fatalf("expected csv export to include raw details_json, got %q", content)
	}

	jsonPath := filepath.Join(t.TempDir(), "details.json")
	jsonCmd := newDetailExportCommand()
	jsonCmd.SetArgs([]string{"--db", dbPath, "--format=json", "--output", jsonPath, "--category=game"})
	if err := jsonCmd.Execute(); err != nil {
		t.Fatalf("json Execute() error = %v", err)
	}
	jsonContent, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("ReadFile(json) error = %v", err)
	}
	if content := string(jsonContent); !strings.Contains(content, "\"Alpha Prime\"") || !strings.Contains(content, "\"details\"") {
		t.Fatalf("expected json export to include detail row, got %q", content)
	}
}

func TestDetailExportCommandRunIDFlatAndSummary(t *testing.T) {
	t.Parallel()

	dbPath := seedDetailReadTestDB(t)

	runJSONPath := filepath.Join(t.TempDir(), "detail-run.json")
	runJSONCmd := newDetailExportCommand()
	runJSONCmd.SetArgs([]string{"--db", dbPath, "--run-id=detail-run-1", "--format=json", "--output", runJSONPath})
	if err := runJSONCmd.Execute(); err != nil {
		t.Fatalf("run json Execute() error = %v", err)
	}
	runJSONContent, err := os.ReadFile(runJSONPath)
	if err != nil {
		t.Fatalf("ReadFile(run json) error = %v", err)
	}
	if content := string(runJSONContent); !strings.Contains(content, "\"first pass\"") || !strings.Contains(content, "\"details\"") {
		t.Fatalf("expected snapshot json export to include first run detail payload, got %q", content)
	}

	flatCSVPath := filepath.Join(t.TempDir(), "detail-flat.csv")
	flatCSVCmd := newDetailExportCommand()
	flatCSVCmd.SetArgs([]string{"--db", dbPath, "--profile=flat", "--format=csv", "--output", flatCSVPath})
	if err := flatCSVCmd.Execute(); err != nil {
		t.Fatalf("flat csv Execute() error = %v", err)
	}
	flatCSVContent, err := os.ReadFile(flatCSVPath)
	if err != nil {
		t.Fatalf("ReadFile(flat csv) error = %v", err)
	}
	if content := string(flatCSVContent); !strings.Contains(content, "genres_csv") || !strings.Contains(content, "RPG") || !strings.Contains(content, "run_id") {
		t.Fatalf("expected flat csv export to include flattened columns, got %q", content)
	}

	summaryPath := filepath.Join(t.TempDir(), "detail-summary.json")
	summaryCmd := newDetailExportCommand()
	summaryCmd.SetArgs([]string{"--db", dbPath, "--run-id=detail-run-2", "--profile=summary", "--format=json", "--output", summaryPath})
	if err := summaryCmd.Execute(); err != nil {
		t.Fatalf("summary Execute() error = %v", err)
	}
	summaryContent, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("ReadFile(summary) error = %v", err)
	}
	if content := string(summaryContent); !strings.Contains(content, "\"run_id\":\"detail-run-2\"") || !strings.Contains(content, "\"with_metascore_count\":1") {
		t.Fatalf("expected summary export to include aggregate metrics, got %q", content)
	}
}

func TestDetailCompareCommandCSVAndJSON(t *testing.T) {
	t.Parallel()

	dbPath := seedDetailReadTestDB(t)

	csvCmd := newDetailCompareCommand()
	var csvOut bytes.Buffer
	csvCmd.SetOut(&csvOut)
	csvCmd.SetErr(&csvOut)
	csvCmd.SetArgs([]string{"--db", dbPath, "--from-run-id=detail-run-1", "--to-run-id=detail-run-2", "--format=csv"})
	if err := csvCmd.Execute(); err != nil {
		t.Fatalf("csv Execute() error = %v", err)
	}
	if output := csvOut.String(); !strings.Contains(output, "change_type") || !strings.Contains(output, "changed") {
		t.Fatalf("expected compare csv output, got %q", output)
	}

	jsonCmd := newDetailCompareCommand()
	var jsonOut bytes.Buffer
	jsonCmd.SetOut(&jsonOut)
	jsonCmd.SetErr(&jsonOut)
	jsonCmd.SetArgs([]string{"--db", dbPath, "--from-run-id=detail-run-1", "--to-run-id=detail-run-2", "--format=json", "--work-href=/game/alpha", "--include-unchanged"})
	if err := jsonCmd.Execute(); err != nil {
		t.Fatalf("json Execute() error = %v", err)
	}
	output := jsonOut.String()
	if !strings.Contains(output, "\"details_json_changed\":true") || !strings.Contains(output, "\"from_details_json\"") {
		t.Fatalf("expected compare json output to include detail json diff metadata, got %q", output)
	}
}

func seedDetailReadTestDB(t *testing.T) string {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-read.db")
	db, err := storage.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	work := domain.Work{
		Name:        "Alpha",
		Href:        "https://www.metacritic.com/game/alpha/",
		ReleaseDate: "Apr 23, 2026",
		Category:    domain.CategoryGame,
	}
	if err := repo.UpsertWork(ctx, work); err != nil {
		t.Fatalf("UpsertWork() error = %v", err)
	}

	startedAt := time.Date(2026, 4, 24, 9, 0, 0, 0, time.UTC)
	if err := repo.CreateDetailCrawlRun(ctx, "detail-run-1", "game", "detail-game", "href=all|force=0|limit=all", startedAt); err != nil {
		t.Fatalf("CreateDetailCrawlRun(run-1) error = %v", err)
	}
	if err := repo.CreateDetailCrawlRun(ctx, "detail-run-2", "game", "detail-game", "href=all|force=0|limit=all", startedAt.Add(time.Hour)); err != nil {
		t.Fatalf("CreateDetailCrawlRun(run-2) error = %v", err)
	}

	firstFetchedAt := startedAt.Add(2 * time.Minute)
	secondFetchedAt := startedAt.Add(time.Hour + 2*time.Minute)
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      work.Href,
		Category:      domain.CategoryGame,
		Title:         "Alpha Prime",
		Metascore:     "90",
		UserScore:     "8.3",
		Rating:        "Rated M",
		Duration:      "42 h",
		Tagline:       "first pass",
		Details:       domain.WorkDetailExtras{Platforms: []string{"PC"}},
		LastFetchedAt: firstFetchedAt,
	}, startedAt.Add(time.Minute), "detail-run-1"); err != nil {
		t.Fatalf("SaveWorkDetail(run-1) error = %v", err)
	}
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:  work.Href,
		Category:  domain.CategoryGame,
		Title:     "Alpha Prime",
		Metascore: "95",
		UserScore: "8.9",
		Rating:    "Rated M",
		Duration:  "44 h",
		Tagline:   "second pass",
		Details: domain.WorkDetailExtras{
			Platforms: []string{"PC"},
			Genres:    []string{"RPG"},
			WhereToBuy: []domain.BuyOption{
				{
					GroupName:    "PC",
					Store:        "Steam",
					LinkURL:      "https://store.steampowered.com/app/alpha",
					PurchaseType: "Buy",
				},
			},
		},
		LastFetchedAt: secondFetchedAt,
	}, startedAt.Add(time.Hour+time.Minute), "detail-run-2"); err != nil {
		t.Fatalf("SaveWorkDetail(run-2) error = %v", err)
	}

	return dbPath
}
