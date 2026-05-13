package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/gofurry/metacritic-harvester/internal/app"
)

func TestCrawlBatchCommandOutputsSummaries(t *testing.T) {
	t.Parallel()

	cmd := newCrawlBatchCommandWithRunner(func(_ context.Context, filePath string, concurrency int) (app.BatchRunResult, error) {
		if filePath != "tasks.yaml" {
			t.Fatalf("unexpected file path %q", filePath)
		}
		if concurrency != 2 {
			t.Fatalf("unexpected concurrency %d", concurrency)
		}
		result := app.BatchRunResult{
			Tasks: []app.BatchTaskResult{
				{
					Name:                  "task-1",
					Kind:                  "list",
					RunID:                 "run-1",
					Category:              "game",
					Metric:                "metascore",
					DBPath:                "output/test.db",
					Success:               true,
					PagesVisited:          2,
					PagesScheduled:        3,
					PagesSucceeded:        2,
					PagesWritten:          2,
					WorksUpserted:         1,
					ListEntriesInserted:   2,
					LatestEntriesUpserted: 2,
				},
				{
					Name:             "task-2",
					Kind:             "detail",
					RunID:            "detail-run-2",
					Category:         "movie",
					DBPath:           "output/test.db",
					Success:          false,
					Error:            "boom",
					Processed:        3,
					Fetched:          1,
					Skipped:          1,
					RecoveredRunning: 1,
					DetailsUpserted:  1,
					Failures:         1,
				},
			},
			TotalTasks:                 2,
			SucceededTasks:             1,
			FailedTasks:                1,
			TotalPagesScheduled:        3,
			TotalPagesSucceeded:        2,
			TotalPagesWritten:          2,
			TotalWorksUpserted:         1,
			TotalListEntriesInserted:   2,
			TotalLatestEntriesUpserted: 2,
			TotalDetailProcessed:       3,
			TotalDetailFetched:         1,
			TotalDetailSkipped:         1,
			TotalRecoveredRunning:      1,
			TotalDetailsUpserted:       1,
			TotalFailures:              1,
		}
		return result, result.Error()
	})

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--file=tasks.yaml", "--concurrency=2"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected batch error because one task failed")
	}

	output := out.String()
	if !strings.Contains(output, "task=task-1 kind=list") || !strings.Contains(output, "task=task-2 kind=detail") {
		t.Fatalf("expected task summaries, got %q", output)
	}
	if !strings.Contains(output, "run_id=run-1") || !strings.Contains(output, "run_id=detail-run-2") {
		t.Fatalf("expected run ids in output, got %q", output)
	}
	if !strings.Contains(output, "pages=2 pages_scheduled=3 pages_succeeded=2 pages_written=2") {
		t.Fatalf("expected list page stats in output, got %q", output)
	}
	if !strings.Contains(output, "processed=3 fetched=1 skipped=1 failed=1 recovered_running=1 details_upserted=1") {
		t.Fatalf("expected detail stats in output, got %q", output)
	}
	if !strings.Contains(output, "batch summary: total=2 succeeded=1 failed=1") {
		t.Fatalf("expected batch summary, got %q", output)
	}
	if !strings.Contains(output, "detail_processed=3 detail_fetched=1 detail_skipped=1 recovered_running=1 details_upserted=1") {
		t.Fatalf("expected detail summary stats in output, got %q", output)
	}
}
