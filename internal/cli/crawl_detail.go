package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/GoFurry/metacritic-harvester/internal/app"
	"github.com/GoFurry/metacritic-harvester/internal/config"
)

func newCrawlDetailCommand() *cobra.Command {
	return newCrawlDetailCommandWithRunner(func(ctx context.Context, cfg config.DetailCommandConfig) (app.DetailRunResult, error) {
		service := app.NewDetailService(app.DetailServiceConfig{
			BaseURL:        config.DefaultBaseURL,
			BackendBaseURL: config.DefaultBackendBaseURL,
			Source:         cfg.Source,
			DBPath:         cfg.DBPath,
			Debug:          cfg.Debug,
			MaxRetries:     cfg.MaxRetries,
			ProxyURLs:      cfg.ProxyURLs,
		})
		return service.Run(ctx, cfg.Task)
	})
}

func newCrawlDetailCommandWithRunner(runner func(context.Context, config.DetailCommandConfig) (app.DetailRunResult, error)) *cobra.Command {
	var opts config.DetailCommandOptions

	cmd := &cobra.Command{
		Use:   "detail",
		Short: "Crawl Metacritic detail pages for known works",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.BuildDetailCommandConfig(opts)
			if err != nil {
				return err
			}
			fmt.Fprintf(
				cmd.ErrOrStderr(),
				"crawl detail starting: category=%s work_href=%s source=%s limit=%d force=%t concurrency=%d db=%s\n",
				cfg.Task.Category,
				cfg.Task.WorkHref,
				cfg.Source,
				cfg.Task.Limit,
				cfg.Task.Force,
				cfg.Concurrency,
				cfg.DBPath,
			)

			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Minute)
			defer cancel()

			result, err := runner(ctx, cfg)
			if err != nil {
				fmt.Fprintf(
					cmd.ErrOrStderr(),
					"crawl detail failed: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s processed=%d/%d fetched=%d skipped=%d failed=%d recovered_running=%d enrich_ok=%d enrich_failed=%d enrich_skipped=%d category=%s work_href=%s db=%s error=%v\n",
					result.RunID,
					result.RequestedSource,
					result.EffectiveSource,
					result.FallbackUsed,
					result.FallbackReason,
					result.Processed,
					result.Total,
					result.Fetched,
					result.Skipped,
					result.Failed,
					result.RecoveredRunning,
					result.EnrichSucceeded,
					result.EnrichFailed,
					result.EnrichSkipped,
					cfg.Task.Category,
					cfg.Task.WorkHref,
					cfg.DBPath,
					err,
				)
				return err
			}

			fmt.Fprintf(
				cmd.OutOrStdout(),
				"crawl detail completed: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s total=%d processed=%d fetched=%d skipped=%d failed=%d recovered_running=%d details_upserted=%d enrich_ok=%d enrich_failed=%d enrich_skipped=%d db=%s\n",
				result.RunID,
				result.RequestedSource,
				result.EffectiveSource,
				result.FallbackUsed,
				result.FallbackReason,
				result.Total,
				result.Processed,
				result.Fetched,
				result.Skipped,
				result.Failed,
				result.RecoveredRunning,
				result.DetailsUpserted,
				result.EnrichSucceeded,
				result.EnrichFailed,
				result.EnrichSkipped,
				cfg.DBPath,
			)
			fmt.Fprintf(
				cmd.ErrOrStderr(),
				"crawl detail finished successfully: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s processed=%d/%d fetched=%d skipped=%d failed=%d recovered_running=%d enrich_ok=%d enrich_failed=%d enrich_skipped=%d db=%s\n",
				result.RunID,
				result.RequestedSource,
				result.EffectiveSource,
				result.FallbackUsed,
				result.FallbackReason,
				result.Processed,
				result.Total,
				result.Fetched,
				result.Skipped,
				result.Failed,
				result.RecoveredRunning,
				result.EnrichSucceeded,
				result.EnrichFailed,
				result.EnrichSkipped,
				cfg.DBPath,
			)
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.Category, "category", "", "Optional category to crawl: game|movie|tv")
	cmd.Flags().StringVar(&opts.WorkHref, "work-href", "", "Optional exact work href to crawl")
	cmd.Flags().StringVar(&opts.Source, "source", string(config.CrawlSourceAPI), "Detail source: api|html|auto")
	cmd.Flags().IntVar(&opts.Limit, "limit", 0, "Maximum number of works to process for detail crawling; 0 means all candidates")
	cmd.Flags().BoolVar(&opts.Force, "force", false, "Refresh details that were already fetched successfully")
	cmd.Flags().IntVar(&opts.Concurrency, "concurrency", 1, "Maximum number of detail pages to fetch concurrently")
	cmd.Flags().StringVar(&opts.DBPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().BoolVar(&opts.Debug, "debug", false, "Enable debug logging")
	cmd.Flags().IntVar(&opts.MaxRetries, "retries", 3, "Maximum retries per request")
	cmd.Flags().StringVar(&opts.Proxies, "proxies", "", "Comma-separated proxy URLs")

	return cmd
}
