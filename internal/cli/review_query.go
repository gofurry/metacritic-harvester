package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newReviewQueryCommand() *cobra.Command {
	var (
		dbPath     string
		category   string
		reviewType string
		platform   string
		workHref   string
		limit      int
		format     string
		checkpoint bool
	)

	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query current latest_reviews rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("category", category),
				scopePart("review_type", reviewType),
				scopePart("platform", platform),
				scopePart("work_href", workHref),
				scopePartInt("limit", limit),
			)
			if err := validateOptionalCategoryMetric(category, ""); err != nil {
				return err
			}
			if strings.TrimSpace(reviewType) != "" {
				if _, err := domain.ParseReviewType(reviewType); err != nil {
					return err
				}
			}
			workHref = domain.NormalizeWorkHref(workHref, config.DefaultBaseURL)
			if format != "table" && format != "json" {
				return fmt.Errorf("format must be table or json")
			}

			repo, closeFn, err := openRepository(cmd.Context(), dbPath, checkpoint)
			if err != nil {
				return wrapReadCommandError("review query", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			rows, err := repo.ListLatestReviews(cmd.Context(), storage.ListLatestReviewsFilter{
				Category:   category,
				ReviewType: reviewType,
				Platform:   platform,
				WorkHref:   workHref,
				Limit:      limit,
			})
			if err != nil {
				return wrapReadCommandError("review query", err, scope)
			}

			mapped := mapLatestReviews(rows)
			if format == "json" {
				return writeJSON(cmd.OutOrStdout(), mapped)
			}

			writer := newTabWriter(cmd.OutOrStdout())
			_, _ = fmt.Fprintln(writer, "REVIEW_KEY\tCATEGORY\tTYPE\tPLATFORM\tSCORE\tPUBLICATION\tAUTHOR\tDATE\tLAST_CRAWLED_AT")
			for _, row := range mapped {
				_, _ = fmt.Fprintf(
					writer,
					"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					row.ReviewKey,
					row.Category,
					row.ReviewType,
					row.PlatformKey,
					row.Score,
					row.PublicationName,
					row.AuthorName,
					row.ReviewDate,
					row.LastCrawledAt,
				)
			}
			return writer.Flush()
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&reviewType, "review-type", "", "Optional review type filter: critic|user")
	cmd.Flags().StringVar(&platform, "platform", "", "Optional platform filter")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum number of rows to return")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table|json")
	addCheckpointFlag(cmd, &checkpoint)

	return cmd
}
