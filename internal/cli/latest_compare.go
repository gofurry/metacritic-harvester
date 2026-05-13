package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newLatestCompareCommand() *cobra.Command {
	var (
		dbPath           string
		fromRunID        string
		toRunID          string
		category         string
		metric           string
		format           string
		includeUnchanged bool
		checkpoint       bool
	)

	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare two crawl runs using snapshot rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("from_run_id", fromRunID),
				scopePart("to_run_id", toRunID),
				scopePart("category", category),
				scopePart("metric", metric),
				scopePart("format", format),
			)
			if err := validateOptionalCategoryMetric(category, metric); err != nil {
				return err
			}
			switch format {
			case "table", "json", "csv":
			default:
				return fmt.Errorf("format must be table, json, or csv")
			}

			repo, closeFn, err := openRepository(cmd.Context(), dbPath, checkpoint)
			if err != nil {
				return wrapReadCommandError("latest compare", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			rows, err := repo.CompareCrawlRuns(cmd.Context(), storage.CompareRunsFilter{
				FromRunID:        fromRunID,
				ToRunID:          toRunID,
				Category:         category,
				Metric:           metric,
				IncludeUnchanged: includeUnchanged,
			})
			if err != nil {
				return wrapReadCommandError("latest compare", err, scope)
			}

			mapped := mapCompareRows(rows)
			switch format {
			case "json":
				return writeJSON(cmd.OutOrStdout(), mapped)
			case "csv":
				csvRows := make([][]string, 0, len(mapped))
				for _, row := range mapped {
					csvRows = append(csvRows, []string{
						row.WorkHref,
						row.Category,
						row.Metric,
						row.FilterKey,
						row.FromRank,
						row.ToRank,
						row.RankDiff,
						row.FromMetascore,
						row.ToMetascore,
						row.MetascoreDiff,
						row.FromUserScore,
						row.ToUserScore,
						row.UserScoreDiff,
						row.ChangeType,
					})
				}
				return writeCSV(cmd.OutOrStdout(), []string{
					"work_href",
					"category",
					"metric",
					"filter_key",
					"from_rank",
					"to_rank",
					"rank_diff",
					"from_metascore",
					"to_metascore",
					"metascore_diff",
					"from_user_score",
					"to_user_score",
					"user_score_diff",
					"change_type",
				}, csvRows)
			default:
				writer := newTabWriter(cmd.OutOrStdout())
				_, _ = fmt.Fprintln(writer, "WORK_HREF\tCATEGORY\tMETRIC\tFILTER_KEY\tFROM_RANK\tTO_RANK\tRANK_DIFF\tFROM_METASCORE\tTO_METASCORE\tMETASCORE_DIFF\tFROM_USER_SCORE\tTO_USER_SCORE\tUSER_SCORE_DIFF\tCHANGE_TYPE")
				for _, row := range mapped {
					_, _ = fmt.Fprintf(
						writer,
						"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
						row.WorkHref,
						row.Category,
						row.Metric,
						row.FilterKey,
						row.FromRank,
						row.ToRank,
						row.RankDiff,
						row.FromMetascore,
						row.ToMetascore,
						row.MetascoreDiff,
						row.FromUserScore,
						row.ToUserScore,
						row.UserScoreDiff,
						row.ChangeType,
					)
				}
				return writer.Flush()
			}
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&fromRunID, "from-run-id", "", "Source crawl run id")
	cmd.Flags().StringVar(&toRunID, "to-run-id", "", "Target crawl run id")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&metric, "metric", "", "Optional metric filter: metascore|userscore|newest")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table|json|csv")
	cmd.Flags().BoolVar(&includeUnchanged, "include-unchanged", false, "Include unchanged rows in the comparison")
	addCheckpointFlag(cmd, &checkpoint)
	_ = cmd.MarkFlagRequired("from-run-id")
	_ = cmd.MarkFlagRequired("to-run-id")

	return cmd
}
