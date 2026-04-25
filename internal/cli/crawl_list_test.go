package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/GoFurry/metacritic-harvester/internal/app"
	"github.com/GoFurry/metacritic-harvester/internal/config"
)

func TestCrawlListCommandParsesFlags(t *testing.T) {
	t.Parallel()

	var captured config.ListCommandConfig
	cmd := newCrawlListCommandWithRunner(func(_ context.Context, cfg config.ListCommandConfig) (app.ListRunResult, error) {
		captured = cfg
		return app.ListRunResult{
			PagesVisited:        1,
			PagesScheduled:      2,
			PagesSucceeded:      1,
			PagesWritten:        1,
			WorksUpserted:       2,
			ListEntriesInserted: 2,
			Failures:            0,
		}, nil
	})

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--category=game",
		"--metric=metascore",
		"--source=auto",
		"--year=2011:2014",
		"--platform=pc,ps5",
		"--genre=action,rpg",
		"--release-type=coming-soon",
		"--pages=2",
		"--db=output/test.db",
		"--retries=4",
		"--proxies=http://127.0.0.1:7897,http://127.0.0.1:7898",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if captured.Task.Category != "game" {
		t.Fatalf("expected category game, got %q", captured.Task.Category)
	}
	if captured.Task.Metric != "metascore" {
		t.Fatalf("expected metric metascore, got %q", captured.Task.Metric)
	}
	if captured.Task.MaxPages != 2 {
		t.Fatalf("expected pages 2, got %d", captured.Task.MaxPages)
	}
	if captured.Source != "auto" {
		t.Fatalf("expected source auto, got %q", captured.Source)
	}
	if captured.Task.Filter.ReleaseYearMin == nil || *captured.Task.Filter.ReleaseYearMin != 2011 {
		t.Fatalf("expected year min 2011, got %+v", captured.Task.Filter.ReleaseYearMin)
	}
	if len(captured.Task.Filter.Platforms) != 2 || len(captured.Task.Filter.Genres) != 2 || len(captured.Task.Filter.ReleaseTypes) != 1 {
		t.Fatalf("unexpected filter parsing: %+v", captured.Task.Filter)
	}
	if captured.MaxRetries != 4 {
		t.Fatalf("expected retries 4, got %d", captured.MaxRetries)
	}
	if len(captured.ProxyURLs) != 2 {
		t.Fatalf("expected 2 proxies, got %d", len(captured.ProxyURLs))
	}
	if !strings.Contains(out.String(), "crawl list completed") {
		t.Fatalf("expected summary output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "source=auto") {
		t.Fatalf("expected source in start output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "pages=1 pages_scheduled=2 pages_succeeded=1 pages_written=1") {
		t.Fatalf("expected page stats in output, got %q", out.String())
	}
}

func TestCrawlListCommandRejectsInvalidCategory(t *testing.T) {
	t.Parallel()

	cmd := newCrawlListCommandWithRunner(func(_ context.Context, _ config.ListCommandConfig) (app.ListRunResult, error) {
		t.Fatal("runner should not be called")
		return app.ListRunResult{}, nil
	})
	cmd.SetArgs([]string{"--category=book", "--metric=metascore"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid category") {
		t.Fatalf("expected invalid category error, got %v", err)
	}
}

func TestCrawlListCommandRejectsUnsupportedFilterFlag(t *testing.T) {
	t.Parallel()

	cmd := newCrawlListCommandWithRunner(func(_ context.Context, _ config.ListCommandConfig) (app.ListRunResult, error) {
		t.Fatal("runner should not be called")
		return app.ListRunResult{}, nil
	})
	cmd.SetArgs([]string{
		"--category=tv",
		"--metric=newest",
		"--release-type=coming-soon",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "release-type is not supported for category tv") {
		t.Fatalf("unexpected error: %v", err)
	}
}
