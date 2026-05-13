package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/app"
	"github.com/gofurry/metacritic-harvester/internal/config"
)

func newCrawlBatchCommand() *cobra.Command {
	return newCrawlBatchCommandWithRunner(func(ctx context.Context, filePath string, concurrency int) (app.BatchRunResult, error) {
		batchFile, err := config.LoadBatchFile(filePath)
		if err != nil {
			return app.BatchRunResult{}, err
		}

		runConfig, err := config.BuildBatchRunConfig(batchFile, concurrency)
		if err != nil {
			return app.BatchRunResult{}, err
		}

		result := app.NewBatchService(config.DefaultBaseURL).RunWithConcurrency(ctx, runConfig.Tasks, runConfig.Concurrency)
		return result, result.Error()
	})
}

func newCrawlBatchCommandWithRunner(runner func(context.Context, string, int) (app.BatchRunResult, error)) *cobra.Command {
	var filePath string
	var concurrency int

	cmd := &cobra.Command{
		Use:   "batch",
		Short: "Run batch crawl tasks from a YAML file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Minute)
			defer cancel()

			result, err := runner(ctx, filePath, concurrency)

			for _, task := range result.Tasks {
				status := "success"
				if !task.Success {
					status = "failed"
				}
				if task.Kind == string(config.BatchTaskKindDetail) {
					fmt.Fprintf(
						cmd.OutOrStdout(),
						"task=%s kind=%s category=%s run_id=%s processed=%d fetched=%d skipped=%d failed=%d recovered_running=%d details_upserted=%d status=%s db=%s\n",
						task.Name,
						task.Kind,
						task.Category,
						task.RunID,
						task.Processed,
						task.Fetched,
						task.Skipped,
						task.Failures,
						task.RecoveredRunning,
						task.DetailsUpserted,
						status,
						task.DBPath,
					)
				} else if task.Kind == string(config.BatchTaskKindReview) {
					fmt.Fprintf(
						cmd.OutOrStdout(),
						"task=%s kind=%s category=%s run_id=%s scopes=%d fetched=%d skipped=%d failed=%d reviews=%d snapshots=%d latest=%d failures=%d status=%s db=%s\n",
						task.Name,
						task.Kind,
						task.Category,
						task.RunID,
						task.ReviewScopesScheduled,
						task.ReviewScopesFetched,
						task.ReviewScopesSkipped,
						task.ReviewScopesFailed,
						task.ReviewsFetched,
						task.ReviewSnapshotsSaved,
						task.ReviewLatestUpserted,
						task.Failures,
						status,
						task.DBPath,
					)
				} else {
					fmt.Fprintf(
						cmd.OutOrStdout(),
						"task=%s kind=%s category=%s metric=%s run_id=%s pages=%d pages_scheduled=%d pages_succeeded=%d pages_written=%d works=%d list_entries=%d latest_entries=%d failures=%d status=%s db=%s\n",
						task.Name,
						task.Kind,
						task.Category,
						task.Metric,
						task.RunID,
						task.PagesVisited,
						task.PagesScheduled,
						task.PagesSucceeded,
						task.PagesWritten,
						task.WorksUpserted,
						task.ListEntriesInserted,
						task.LatestEntriesUpserted,
						task.Failures,
						status,
						task.DBPath,
					)
				}
				if task.Error != "" {
					fmt.Fprintf(cmd.OutOrStdout(), "task=%s error=%s\n", task.Name, task.Error)
				}
			}

			fmt.Fprintf(
				cmd.OutOrStdout(),
				"batch summary: total=%d succeeded=%d failed=%d pages_scheduled=%d pages_succeeded=%d pages_written=%d works=%d list_entries=%d latest_entries=%d detail_processed=%d detail_fetched=%d detail_skipped=%d recovered_running=%d details_upserted=%d review_scopes=%d review_fetched=%d review_skipped=%d review_failed=%d reviews=%d review_snapshots=%d review_latest=%d failures=%d\n",
				result.TotalTasks,
				result.SucceededTasks,
				result.FailedTasks,
				result.TotalPagesScheduled,
				result.TotalPagesSucceeded,
				result.TotalPagesWritten,
				result.TotalWorksUpserted,
				result.TotalListEntriesInserted,
				result.TotalLatestEntriesUpserted,
				result.TotalDetailProcessed,
				result.TotalDetailFetched,
				result.TotalDetailSkipped,
				result.TotalRecoveredRunning,
				result.TotalDetailsUpserted,
				result.TotalReviewScopesScheduled,
				result.TotalReviewScopesFetched,
				result.TotalReviewScopesSkipped,
				result.TotalReviewScopesFailed,
				result.TotalReviewsFetched,
				result.TotalReviewSnapshotsSaved,
				result.TotalReviewLatestUpserted,
				result.TotalFailures,
			)

			return err
		},
	}

	cmd.Flags().StringVar(&filePath, "file", "", "YAML batch task file path")
	cmd.Flags().IntVar(&concurrency, "concurrency", 1, "Maximum number of tasks to run concurrently")
	_ = cmd.MarkFlagRequired("file")

	return cmd
}
