package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/GoFurry/metacritic-harvester/internal/app"
	"github.com/GoFurry/metacritic-harvester/internal/config"
)

func newCrawlReviewsCommand() *cobra.Command {
	var opts config.ReviewCommandOptions

	cmd := &cobra.Command{
		Use:   "reviews",
		Short: "Crawl review data from Metacritic backend APIs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.BuildReviewCommandConfig(opts)
			if err != nil {
				return err
			}
			fmt.Fprintf(
				cmd.ErrOrStderr(),
				"crawl reviews starting: category=%s work_href=%s source=api review_type=%s sentiment=%s sort=%s platform=%s limit=%d page_size=%d max_pages=%d force=%t concurrency=%d db=%s\n",
				cfg.Task.Category,
				cfg.Task.WorkHref,
				cfg.Task.ReviewType,
				cfg.Task.Sentiment,
				cfg.Task.Sort,
				cfg.Task.Platform,
				cfg.Task.Limit,
				cfg.Task.PageSize,
				cfg.Task.MaxPages,
				cfg.Task.Force,
				cfg.Task.Concurrency,
				cfg.DBPath,
			)

			ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Minute)
			defer cancel()

			service := app.NewReviewService(app.ReviewServiceConfig{
				BaseURL:    config.DefaultBackendBaseURL,
				DBPath:     cfg.DBPath,
				Debug:      cfg.Debug,
				MaxRetries: cfg.MaxRetries,
				ProxyURLs:  cfg.ProxyURLs,
			})

			result, err := service.Run(ctx, cfg.Task)
			if err != nil {
				fmt.Fprintf(
					cmd.ErrOrStderr(),
					"crawl reviews failed: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s candidates=%d scopes=%d fetched=%d skipped=%d failed=%d reviews=%d snapshots=%d latest=%d failures=%d db=%s error=%v\n",
					result.RunID,
					result.RequestedSource,
					result.EffectiveSource,
					result.FallbackUsed,
					result.FallbackReason,
					result.Candidates,
					result.ScopesScheduled,
					result.ScopesFetched,
					result.ScopesSkipped,
					result.ScopesFailed,
					result.ReviewsFetched,
					result.ReviewSnapshotsSaved,
					result.LatestReviewsUpserted,
					result.Failures,
					cfg.DBPath,
					err,
				)
			}
			fmt.Fprintf(
				cmd.OutOrStdout(),
				"reviews summary: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s candidates=%d scopes=%d fetched=%d skipped=%d failed=%d reviews=%d snapshots=%d latest=%d failures=%d\n",
				result.RunID,
				result.RequestedSource,
				result.EffectiveSource,
				result.FallbackUsed,
				result.FallbackReason,
				result.Candidates,
				result.ScopesScheduled,
				result.ScopesFetched,
				result.ScopesSkipped,
				result.ScopesFailed,
				result.ReviewsFetched,
				result.ReviewSnapshotsSaved,
				result.LatestReviewsUpserted,
				result.Failures,
			)
			return err
		},
	}

	cmd.Flags().StringVar(&opts.Category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&opts.WorkHref, "work-href", "", "Optional work href filter")
	cmd.Flags().IntVar(&opts.Limit, "limit", 0, "Maximum number of works to process for review crawling; 0 means all candidates")
	cmd.Flags().BoolVar(&opts.Force, "force", false, "Re-crawl scopes even if they already succeeded")
	cmd.Flags().IntVar(&opts.Concurrency, "concurrency", 1, "Maximum number of work scopes to run concurrently")
	cmd.Flags().StringVar(&opts.ReviewType, "review-type", "all", "Review type: critic|user|all")
	cmd.Flags().StringVar(&opts.Sentiment, "sentiment", "all", "Review sentiment filter: all|positive|neutral|negative")
	cmd.Flags().StringVar(&opts.Sort, "sort", "", "Optional review sort: date|score|publication")
	cmd.Flags().StringVar(&opts.Platform, "platform", "", "Optional game platform scope, e.g. pc or xbox-360")
	cmd.Flags().IntVar(&opts.PageSize, "page-size", 20, "Reviews page size")
	cmd.Flags().IntVar(&opts.MaxPages, "max-pages", 0, "Maximum number of pages to fetch per scope")
	cmd.Flags().StringVar(&opts.DBPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().BoolVar(&opts.Debug, "debug", false, "Enable debug logging")
	cmd.Flags().IntVar(&opts.MaxRetries, "retries", 3, "Maximum retry attempts for HTTP requests")
	cmd.Flags().StringVar(&opts.Proxies, "proxies", "", "Comma-separated proxy URLs")

	return cmd
}
