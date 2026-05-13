package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newLatestExportCommand() *cobra.Command {
	var (
		dbPath     string
		category   string
		metric     string
		workHref   string
		filterKey  string
		runID      string
		format     string
		profile    string
		output     string
		checkpoint bool
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export current latest_list_entries rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("category", category),
				scopePart("metric", metric),
				scopePart("work_href", workHref),
				scopePart("filter_key", filterKey),
				scopePart("run_id", runID),
				scopePart("profile", profile),
				scopePart("format", format),
			)
			if err := validateOptionalCategoryMetric(category, metric); err != nil {
				return err
			}
			workHref = domain.NormalizeWorkHref(workHref, config.DefaultBaseURL)
			if format != "csv" && format != "json" {
				return fmt.Errorf("format must be csv or json")
			}
			if !isValidExportProfile(profile) {
				return fmt.Errorf("profile must be raw, flat, or summary")
			}
			if output == "" {
				return fmt.Errorf("output must not be empty")
			}

			repo, closeFn, err := openRepository(cmd.Context(), dbPath, checkpoint)
			if err != nil {
				return wrapReadCommandError("latest export", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			filter := storage.ListLatestEntriesFilter{
				Category:  category,
				Metric:    metric,
				WorkHref:  workHref,
				FilterKey: filterKey,
				Limit:     -1,
			}
			var rows []latestEntryView
			if runID == "" {
				entries, err := repo.ListLatestEntries(cmd.Context(), filter)
				if err != nil {
					return wrapReadCommandError("latest export", err, scope)
				}
				rows = mapLatestEntries(entries)
			} else {
				entries, err := repo.ListListEntriesByRun(cmd.Context(), runID, filter)
				if err != nil {
					return wrapReadCommandError("latest export", err, scope)
				}
				rows = mapListEntries(entries)
			}

			file, err := createOutputFile(output)
			if err != nil {
				return wrapReadCommandError("latest export", err, scope, scopePart("output", output))
			}
			defer file.Close()

			if profile == exportProfileSummary {
				summary := summarizeLatestEntries(rows)
				if format == "json" {
					return writeJSON(file, summary)
				}
				csvRows := make([][]string, 0, len(summary))
				for _, row := range summary {
					csvRows = append(csvRows, []string{
						row.RunID,
						row.Category,
						row.Metric,
						row.FilterKey,
						fmt.Sprintf("%d", row.RowCount),
						fmt.Sprintf("%d", row.DistinctWorkCount),
						fmt.Sprintf("%d", row.MinRank),
						fmt.Sprintf("%d", row.MaxRank),
					})
				}
				return writeCSV(file, []string{
					"run_id",
					"category",
					"metric",
					"filter_key",
					"row_count",
					"distinct_work_count",
					"min_rank",
					"max_rank",
				}, csvRows)
			}

			if format == "json" {
				return writeJSON(file, rows)
			}

			csvRows := make([][]string, 0, len(rows))
			for _, entry := range rows {
				csvRows = append(csvRows, []string{
					entry.WorkHref,
					entry.Category,
					entry.Metric,
					entry.FilterKey,
					fmt.Sprintf("%d", entry.PageNo),
					fmt.Sprintf("%d", entry.RankNo),
					entry.Metascore,
					entry.UserScore,
					entry.LastCrawledAt,
					entry.SourceCrawlRunID,
				})
			}

			return writeCSV(file, []string{
				"work_href",
				"category",
				"metric",
				"filter_key",
				"page_no",
				"rank_no",
				"metascore",
				"user_score",
				"last_crawled_at",
				"source_crawl_run_id",
			}, csvRows)
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&metric, "metric", "", "Optional metric filter: metascore|userscore|newest")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().StringVar(&filterKey, "filter-key", "", "Optional normalized filter key")
	cmd.Flags().StringVar(&runID, "run-id", "", "Optional crawl run id; when set, export snapshot rows from list_entries")
	cmd.Flags().StringVar(&format, "format", "csv", "Export format: csv|json")
	cmd.Flags().StringVar(&profile, "profile", exportProfileRaw, "Export profile: raw|flat|summary")
	cmd.Flags().StringVar(&output, "output", "", "Output file path")
	addCheckpointFlag(cmd, &checkpoint)
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
