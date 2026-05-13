package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newLatestQueryCommand() *cobra.Command {
	var (
		dbPath     string
		category   string
		metric     string
		workHref   string
		filterKey  string
		limit      int
		format     string
		checkpoint bool
	)

	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query current latest_list_entries rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("category", category),
				scopePart("metric", metric),
				scopePart("work_href", workHref),
				scopePart("filter_key", filterKey),
				scopePartInt("limit", limit),
			)
			if err := validateOptionalCategoryMetric(category, metric); err != nil {
				return err
			}
			if format != "table" && format != "json" {
				return fmt.Errorf("format must be table or json")
			}

			repo, closeFn, err := openRepository(cmd.Context(), dbPath, checkpoint)
			if err != nil {
				return wrapReadCommandError("latest query", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			entries, err := repo.ListLatestEntries(cmd.Context(), storage.ListLatestEntriesFilter{
				Category:  category,
				Metric:    metric,
				WorkHref:  workHref,
				FilterKey: filterKey,
				Limit:     limit,
			})
			if err != nil {
				return wrapReadCommandError("latest query", err, scope)
			}

			mapped := mapLatestEntries(entries)
			if format == "json" {
				return writeJSON(cmd.OutOrStdout(), mapped)
			}

			writer := newTabWriter(cmd.OutOrStdout())
			_, _ = fmt.Fprintln(writer, "WORK_HREF\tCATEGORY\tMETRIC\tFILTER_KEY\tPAGE\tRANK\tMETASCORE\tUSER_SCORE\tLAST_CRAWLED_AT\tSOURCE_RUN_ID")
			for _, entry := range mapped {
				_, _ = fmt.Fprintf(
					writer,
					"%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n",
					entry.WorkHref,
					entry.Category,
					entry.Metric,
					entry.FilterKey,
					entry.PageNo,
					entry.RankNo,
					entry.Metascore,
					entry.UserScore,
					entry.LastCrawledAt,
					entry.SourceCrawlRunID,
				)
			}
			return writer.Flush()
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&metric, "metric", "", "Optional metric filter: metascore|userscore|newest")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().StringVar(&filterKey, "filter-key", "", "Optional normalized filter key")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum number of rows to return")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table|json")
	addCheckpointFlag(cmd, &checkpoint)

	return cmd
}
