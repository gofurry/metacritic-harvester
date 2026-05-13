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

func TestCrawlReviewsCommandParsesFlags(t *testing.T) {
	t.Parallel()

	var captured config.ReviewCommandConfig
	cmd := newCrawlReviewsCommandWithRunner(func(_ context.Context, cfg config.ReviewCommandConfig) (app.ReviewRunResult, error) {
		captured = cfg
		return app.ReviewRunResult{
			RunID:                 "review-run-1",
			RequestedSource:       "api",
			EffectiveSource:       "api",
			Candidates:            2,
			ScopesScheduled:       3,
			ScopesFetched:         2,
			ScopesSkipped:         1,
			ReviewsFetched:        20,
			ReviewSnapshotsSaved:  20,
			LatestReviewsUpserted: 20,
		}, nil
	})

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--category=game",
		"--work-href=/game/test-game",
		"--limit=5",
		"--force",
		"--concurrency=3",
		"--review-type=critic",
		"--sentiment=positive",
		"--sort=score",
		"--platform=pc",
		"--page-size=50",
		"--max-pages=4",
		"--db=output/reviews.db",
		"--timeout=45m",
		"--continue-on-error=false",
		"--rps=3.5",
		"--burst=6",
		"--retries=4",
		"--proxies=http://127.0.0.1:7897",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if captured.Task.Category != "game" || captured.Task.ReviewType != "critic" {
		t.Fatalf("unexpected task core fields: %+v", captured.Task)
	}
	if captured.Task.WorkHref != "https://www.metacritic.com/game/test-game" {
		t.Fatalf("unexpected work href: %q", captured.Task.WorkHref)
	}
	if captured.Task.Sentiment != "positive" || captured.Task.Sort != "score" || captured.Task.Platform != "pc" {
		t.Fatalf("unexpected review filters: %+v", captured.Task)
	}
	if captured.Task.PageSize != 50 || captured.Task.MaxPages != 4 || captured.Task.Concurrency != 3 {
		t.Fatalf("unexpected paging/concurrency: %+v", captured.Task)
	}
	if captured.Timeout != 45*time.Minute || captured.ContinueOnError {
		t.Fatalf("unexpected runtime config: timeout=%s continue_on_error=%t", captured.Timeout, captured.ContinueOnError)
	}
	if captured.RPS != 3.5 || captured.Burst != 6 {
		t.Fatalf("unexpected rate config: rps=%v burst=%d", captured.RPS, captured.Burst)
	}
	if captured.MaxRetries != 4 || len(captured.ProxyURLs) != 1 {
		t.Fatalf("unexpected network config: %+v", captured)
	}
	if !strings.Contains(out.String(), "timeout=45m0s continue_on_error=false rps=3.50 burst=6") {
		t.Fatalf("expected runtime flags in output, got %q", out.String())
	}
	if !strings.Contains(out.String(), "reviews summary: run_id=review-run-1") {
		t.Fatalf("expected summary output, got %q", out.String())
	}
}

func TestCrawlReviewsCommandReturnsRunnerError(t *testing.T) {
	t.Parallel()

	cmd := newCrawlReviewsCommandWithRunner(func(_ context.Context, _ config.ReviewCommandConfig) (app.ReviewRunResult, error) {
		return app.ReviewRunResult{
			RunID:           "review-run-failed",
			RequestedSource: "api",
			EffectiveSource: "api",
			ScopesScheduled: 1,
			ScopesFailed:    1,
			Failures:        1,
		}, errors.New("review failed")
	})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--review-type=critic", "--db=output/reviews.db"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(out.String(), "crawl reviews failed: run_id=review-run-failed requested_source=api effective_source=api") {
		t.Fatalf("expected failure summary in output, got %q", out.String())
	}
}
