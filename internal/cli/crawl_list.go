package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/GoFurry/metacritic-harvester/internal/app"
	"github.com/GoFurry/metacritic-harvester/internal/config"
)

func newCrawlListCommand() *cobra.Command {
	return newCrawlListCommandWithRunner(func(ctx context.Context, cfg config.ListCommandConfig) (app.ListRunResult, error) {
		service := app.NewListService(app.ListServiceConfig{
			BaseURL:        config.DefaultBaseURL,
			BackendBaseURL: config.DefaultBackendBaseURL,
			Source:         cfg.Source,
			DBPath:         cfg.DBPath,
			Debug:          cfg.Debug,
			MaxRetries:     cfg.MaxRetries,
			ProxyURLs:      cfg.ProxyURLs,
			RunSource:      "crawl list",
			TaskName:       fmt.Sprintf("%s-%s", cfg.Task.Category, cfg.Task.Metric),
		})
		return service.Run(ctx, cfg.Task)
	})
}

func newCrawlListCommandWithRunner(runner func(context.Context, config.ListCommandConfig) (app.ListRunResult, error)) *cobra.Command {
	var opts config.ListCommandOptions

	cmd := &cobra.Command{
		Use:   "list",
		Short: "Crawl a Metacritic list into SQLite",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.BuildListCommandConfig(opts)
			if err != nil {
				return err
			}
			fmt.Fprintf(
				cmd.ErrOrStderr(),
				"crawl list starting: category=%s metric=%s source=%s pages=%d db=%s\n",
				cfg.Task.Category,
				cfg.Task.Metric,
				cfg.Source,
				cfg.Task.MaxPages,
				cfg.DBPath,
			)

			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Minute)
			defer cancel()

			result, err := runner(ctx, cfg)
			if err != nil {
				fmt.Fprintf(
					cmd.ErrOrStderr(),
					"crawl list failed: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s category=%s metric=%s db=%s error=%v\n",
					result.RunID,
					result.RequestedSource,
					result.EffectiveSource,
					result.FallbackUsed,
					result.FallbackReason,
					cfg.Task.Category,
					cfg.Task.Metric,
					cfg.DBPath,
					err,
				)
				return err
			}

			fmt.Fprintf(
				cmd.OutOrStdout(),
				"crawl list completed: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s category=%s metric=%s pages=%d pages_scheduled=%d pages_succeeded=%d pages_written=%d works=%d list_entries=%d latest_entries=%d failures=%d db=%s\n",
				result.RunID,
				result.RequestedSource,
				result.EffectiveSource,
				result.FallbackUsed,
				result.FallbackReason,
				cfg.Task.Category,
				cfg.Task.Metric,
				result.PagesVisited,
				result.PagesScheduled,
				result.PagesSucceeded,
				result.PagesWritten,
				result.WorksUpserted,
				result.ListEntriesInserted,
				result.LatestEntriesUpserted,
				result.Failures,
				cfg.DBPath,
			)
			fmt.Fprintf(
				cmd.ErrOrStderr(),
				"crawl list finished successfully: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s category=%s metric=%s db=%s\n",
				result.RunID,
				result.RequestedSource,
				result.EffectiveSource,
				result.FallbackUsed,
				result.FallbackReason,
				cfg.Task.Category,
				cfg.Task.Metric,
				cfg.DBPath,
			)
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.Category, "category", "", "Category to crawl: game|movie|tv")
	cmd.Flags().StringVar(&opts.Metric, "metric", "", "Metric to crawl: metascore|userscore|newest")
	cmd.Flags().StringVar(&opts.Source, "source", string(config.CrawlSourceAPI), "List source: api|html|auto")
	cmd.Flags().StringVar(&opts.Year, "year", "", "Release year range in YYYY:YYYY format")
	cmd.Flags().StringVar(&opts.Platform, "platform", "", "Comma-separated game platforms; game only")
	cmd.Flags().StringVar(&opts.Network, "network", "", "Comma-separated movie/tv networks; movie|tv only")
	cmd.Flags().StringVar(&opts.Genre, "genre", "", "Comma-separated genres")
	cmd.Flags().StringVar(&opts.ReleaseType, "release-type", "", "Comma-separated release types; game|movie only")
	cmd.Flags().IntVar(&opts.Pages, "pages", 0, "Maximum number of pages to crawl; 0 means all pages")
	cmd.Flags().StringVar(&opts.DBPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().BoolVar(&opts.Debug, "debug", false, "Enable debug logging")
	cmd.Flags().IntVar(&opts.MaxRetries, "retries", 3, "Maximum retries per request")
	cmd.Flags().StringVar(&opts.Proxies, "proxies", "", "Comma-separated proxy URLs")

	_ = cmd.MarkFlagRequired("category")
	_ = cmd.MarkFlagRequired("metric")

	return cmd
}
