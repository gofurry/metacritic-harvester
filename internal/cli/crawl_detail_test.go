package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/app"
	"github.com/gofurry/metacritic-harvester/internal/config"
)

func TestCrawlDetailCommandParsesFlags(t *testing.T) {
	t.Parallel()

	var captured config.DetailCommandConfig
	cmd := newCrawlDetailCommandWithRunner(func(_ context.Context, cfg config.DetailCommandConfig) (app.DetailRunResult, error) {
		captured = cfg
		return app.DetailRunResult{
			RunID:            "detail-run-1",
			RequestedSource:  "auto",
			EffectiveSource:  "api",
			Total:            3,
			Processed:        3,
			Fetched:          2,
			Skipped:          1,
			RecoveredRunning: 1,
			DetailsUpserted:  2,
		}, nil
	})

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--category=game",
		"--work-href=/game/test-game",
		"--source=auto",
		"--limit=3",
		"--force",
		"--concurrency=4",
		"--db=output/detail.db",
		"--timeout=90m",
		"--continue-on-error=false",
		"--rps=6",
		"--burst=9",
		"--retries=4",
		"--proxies=http://127.0.0.1:7897",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if captured.Task.Category != "game" {
		t.Fatalf("expected category game, got %q", captured.Task.Category)
	}
	if captured.Task.WorkHref != "https://www.metacritic.com/game/test-game" {
		t.Fatalf("unexpected work href: %q", captured.Task.WorkHref)
	}
	if captured.Task.Limit != 3 || !captured.Task.Force {
		t.Fatalf("unexpected task options: %+v", captured.Task)
	}
	if captured.Source != "auto" {
		t.Fatalf("expected source auto, got %q", captured.Source)
	}
	if captured.Task.Concurrency != 4 || captured.Concurrency != 4 {
		t.Fatalf("unexpected concurrency: task=%d config=%d", captured.Task.Concurrency, captured.Concurrency)
	}
	if captured.Timeout != 90*time.Minute || captured.ContinueOnError {
		t.Fatalf("unexpected runtime config: timeout=%s continue_on_error=%t", captured.Timeout, captured.ContinueOnError)
	}
	if captured.RPS != 6 || captured.Burst != 9 {
		t.Fatalf("unexpected rate config: rps=%v burst=%d", captured.RPS, captured.Burst)
	}
	if captured.MaxRetries != 4 || len(captured.ProxyURLs) != 1 {
		t.Fatalf("unexpected network options: retries=%d proxies=%+v", captured.MaxRetries, captured.ProxyURLs)
	}
	normalizedOutput := strings.ReplaceAll(out.String(), "\\", "/")
	if !strings.Contains(normalizedOutput, "crawl detail starting: category=game work_href=https://www.metacritic.com/game/test-game source=auto limit=3 force=true concurrency=4 timeout=1h30m0s continue_on_error=false rps=6.00 burst=9 db=output/detail.db") {
		t.Fatalf("unexpected start output: %q", out.String())
	}
	if !strings.Contains(normalizedOutput, "crawl detail completed: run_id=detail-run-1 requested_source=auto effective_source=api fallback_used=false fallback_reason= total=3 processed=3 fetched=2 skipped=1 failed=0 recovered_running=1 details_upserted=2") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestCrawlDetailCommandRejectsInvalidCategory(t *testing.T) {
	t.Parallel()

	cmd := newCrawlDetailCommandWithRunner(func(_ context.Context, _ config.DetailCommandConfig) (app.DetailRunResult, error) {
		t.Fatal("runner should not be called")
		return app.DetailRunResult{}, nil
	})
	cmd.SetArgs([]string{"--category=book"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "invalid category") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCrawlDetailCommandReturnsRunnerError(t *testing.T) {
	t.Parallel()

	cmd := newCrawlDetailCommandWithRunner(func(_ context.Context, _ config.DetailCommandConfig) (app.DetailRunResult, error) {
		return app.DetailRunResult{
			RunID:           "detail-run-failed",
			RequestedSource: "api",
			EffectiveSource: "api",
			Total:           1,
			Processed:       1,
			Failed:          1,
			Failures:        1,
		}, errors.New("detail failed")
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db=output/detail.db"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(out.String(), "crawl detail completed") {
		t.Fatalf("did not expect success output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "crawl detail failed: run_id=detail-run-failed requested_source=api effective_source=api") {
		t.Fatalf("expected failure summary in output, got %q", out.String())
	}
}
