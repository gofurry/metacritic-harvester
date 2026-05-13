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

func TestReviewQueryCommandJSON(t *testing.T) {
	t.Parallel()

	dbPath := seedReviewReadTestDB(t)

	cmd := newReviewQueryCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath, "--category=game", "--format=json"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "\"review_type\":\"critic\"") || !strings.Contains(output, "\"review_type\":\"user\"") {
		t.Fatalf("expected review query json output to include critic and user rows, got %q", output)
	}
}

func TestReviewQueryCommandRequiresExistingDB(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "missing", "review.db")
	cmd := newReviewQueryCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath})

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected Execute() to fail for missing database")
	}
	if output := out.String(); !strings.Contains(output, "review query failed") || !strings.Contains(output, "db=") {
		t.Fatalf("expected wrapped review query error, got %q", output)
	}
}

func TestReviewExportCommandRunIDFlatAndSummary(t *testing.T) {
	t.Parallel()

	dbPath := seedReviewReadTestDB(t)

	rawPath := filepath.Join(t.TempDir(), "review-run.json")
	rawCmd := newReviewExportCommand()
	rawCmd.SetArgs([]string{"--db", dbPath, "--run-id=review-run-1", "--format=json", "--output", rawPath})
	if err := rawCmd.Execute(); err != nil {
		t.Fatalf("raw Execute() error = %v", err)
	}
	rawContent, err := os.ReadFile(rawPath)
	if err != nil {
		t.Fatalf("ReadFile(raw) error = %v", err)
	}
	rawOutput := string(rawContent)
	if !strings.Contains(rawOutput, "\"source_crawl_run_id\":\"review-run-1\"") || !strings.Contains(rawOutput, "\"publication_name\":\"Game Weekly\"") {
		t.Fatalf("expected snapshot raw export to include run-1 review row, got %q", rawOutput)
	}

	flatPath := filepath.Join(t.TempDir(), "review-flat.csv")
	flatCmd := newReviewExportCommand()
	flatCmd.SetArgs([]string{"--db", dbPath, "--profile=flat", "--format=csv", "--output", flatPath})
	if err := flatCmd.Execute(); err != nil {
		t.Fatalf("flat Execute() error = %v", err)
	}
	flatContent, err := os.ReadFile(flatPath)
	if err != nil {
		t.Fatalf("ReadFile(flat) error = %v", err)
	}
	flatOutput := string(flatContent)
	if !strings.Contains(flatOutput, "run_id") || !strings.Contains(flatOutput, "critic") || !strings.Contains(flatOutput, "user") {
		t.Fatalf("expected flat export to include run_id and standardized rows, got %q", flatOutput)
	}
	if strings.Contains(flatOutput, "source_payload_json") {
		t.Fatalf("expected flat export to omit source_payload_json, got %q", flatOutput)
	}

	summaryPath := filepath.Join(t.TempDir(), "review-summary.json")
	summaryCmd := newReviewExportCommand()
	summaryCmd.SetArgs([]string{"--db", dbPath, "--profile=summary", "--format=json", "--output", summaryPath})
	if err := summaryCmd.Execute(); err != nil {
		t.Fatalf("summary Execute() error = %v", err)
	}
	summaryContent, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("ReadFile(summary) error = %v", err)
	}
	summaryOutput := string(summaryContent)
	if !strings.Contains(summaryOutput, "\"with_publication_count\"") || !strings.Contains(summaryOutput, "\"with_username_count\"") {
		t.Fatalf("expected summary export to include aggregate counters, got %q", summaryOutput)
	}
}

func TestReviewCompareCommandStillWorks(t *testing.T) {
	t.Parallel()

	dbPath := seedReviewReadTestDB(t)

	cmd := newReviewCompareCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db", dbPath, "--from-run-id=review-run-1", "--to-run-id=review-run-2", "--format=csv"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "change_type") || !strings.Contains(output, "changed") {
		t.Fatalf("expected compare csv output, got %q", output)
	}
}

func seedReviewReadTestDB(t *testing.T) string {
	t.Helper()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "review-read.db")
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

	startedAt := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	if err := repo.CreateReviewCrawlRun(ctx, "review-run-1", "crawl reviews", "reviews-game", "game", "game|critic", startedAt); err != nil {
		t.Fatalf("CreateReviewCrawlRun(run-1) error = %v", err)
	}
	if err := repo.CreateReviewCrawlRun(ctx, "review-run-2", "crawl reviews", "reviews-game", "game", "game|critic", startedAt.Add(time.Hour)); err != nil {
		t.Fatalf("CreateReviewCrawlRun(run-2) error = %v", err)
	}

	criticScore1 := 9.0
	criticScore2 := 9.5
	userScore := 8.0
	thumbsUp := int64(12)
	thumbsDown := int64(1)
	spoiler := false

	criticKey := domain.BuildCriticReviewKey(work.Href, domain.CategoryGame, "pc", "game-weekly", "2026-04-24", "Excellent")
	userKey := domain.BuildUserReviewKey(work.Href, domain.CategoryGame, "pc", "user-1", "alphaFan", "2026-04-24", &userScore, "Loved it")

	run1Records := []domain.ReviewRecord{
		{
			ReviewKey:         criticKey,
			ExternalReviewID:  "critic-1",
			CrawlRunID:        "review-run-1",
			WorkHref:          work.Href,
			Category:          domain.CategoryGame,
			ReviewType:        domain.ReviewTypeCritic,
			PlatformKey:       "pc",
			ReviewURL:         "https://www.metacritic.com/game/alpha/reviews/critic-1",
			ReviewDate:        "2026-04-24",
			Score:             &criticScore1,
			Quote:             "Excellent",
			PublicationName:   "Game Weekly",
			PublicationSlug:   "game-weekly",
			AuthorName:        "Alex Critic",
			AuthorSlug:        "alex-critic",
			SourcePayloadJSON: `{"kind":"critic"}`,
			CrawledAt:         startedAt.Add(2 * time.Minute),
		},
	}
	if err := repo.SaveReviewRecords(ctx, run1Records); err != nil {
		t.Fatalf("SaveReviewRecords(run-1) error = %v", err)
	}

	run2Records := []domain.ReviewRecord{
		{
			ReviewKey:         criticKey,
			ExternalReviewID:  "critic-1",
			CrawlRunID:        "review-run-2",
			WorkHref:          work.Href,
			Category:          domain.CategoryGame,
			ReviewType:        domain.ReviewTypeCritic,
			PlatformKey:       "pc",
			ReviewURL:         "https://www.metacritic.com/game/alpha/reviews/critic-1",
			ReviewDate:        "2026-04-24",
			Score:             &criticScore2,
			Quote:             "Excellent",
			PublicationName:   "Game Weekly",
			PublicationSlug:   "game-weekly",
			AuthorName:        "Alex Critic",
			AuthorSlug:        "alex-critic",
			SourcePayloadJSON: `{"kind":"critic","updated":true}`,
			CrawledAt:         startedAt.Add(time.Hour + 2*time.Minute),
		},
		{
			ReviewKey:         userKey,
			ExternalReviewID:  "user-1",
			CrawlRunID:        "review-run-2",
			WorkHref:          work.Href,
			Category:          domain.CategoryGame,
			ReviewType:        domain.ReviewTypeUser,
			PlatformKey:       "pc",
			ReviewURL:         "https://www.metacritic.com/game/alpha/user-reviews/user-1",
			ReviewDate:        "2026-04-24",
			Score:             &userScore,
			Quote:             "Loved it",
			Username:          "alphaFan",
			UserSlug:          "alphafan",
			ThumbsUp:          &thumbsUp,
			ThumbsDown:        &thumbsDown,
			VersionLabel:      "1.0",
			SpoilerFlag:       &spoiler,
			SourcePayloadJSON: `{"kind":"user"}`,
			CrawledAt:         startedAt.Add(time.Hour + 3*time.Minute),
		},
	}
	if err := repo.SaveReviewRecords(ctx, run2Records); err != nil {
		t.Fatalf("SaveReviewRecords(run-2) error = %v", err)
	}

	return dbPath
}
