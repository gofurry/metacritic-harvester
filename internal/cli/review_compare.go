package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newReviewCompareCommand() *cobra.Command {
	var (
		dbPath           string
		fromRunID        string
		toRunID          string
		category         string
		reviewType       string
		platform         string
		workHref         string
		format           string
		includeUnchanged bool
		checkpoint       bool
	)

	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare two review crawl runs using snapshot rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("from_run_id", fromRunID),
				scopePart("to_run_id", toRunID),
				scopePart("category", category),
				scopePart("review_type", reviewType),
				scopePart("platform", platform),
				scopePart("work_href", workHref),
				scopePart("format", format),
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
			switch format {
			case "table", "json", "csv":
			default:
				return fmt.Errorf("format must be table, json, or csv")
			}

			repo, closeFn, err := openRepository(cmd.Context(), dbPath, checkpoint)
			if err != nil {
				return wrapReadCommandError("review compare", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			rows, err := repo.CompareReviewSnapshots(cmd.Context(), storage.CompareReviewsFilter{
				FromRunID:        fromRunID,
				ToRunID:          toRunID,
				Category:         category,
				ReviewType:       reviewType,
				Platform:         platform,
				WorkHref:         workHref,
				IncludeUnchanged: includeUnchanged,
			})
			if err != nil {
				return wrapReadCommandError("review compare", err, scope)
			}

			mapped := mapReviewCompareRows(rows)
			switch format {
			case "json":
				return writeJSON(cmd.OutOrStdout(), mapped)
			case "csv":
				csvRows := make([][]string, 0, len(mapped))
				for _, row := range mapped {
					csvRows = append(csvRows, []string{
						row.ReviewKey,
						row.WorkHref,
						row.Category,
						row.ReviewType,
						row.PlatformKey,
						row.FromScore,
						row.ToScore,
						row.ScoreDiff,
						row.FromQuote,
						row.ToQuote,
						row.FromThumbsUp,
						row.ToThumbsUp,
						row.FromThumbsDown,
						row.ToThumbsDown,
						row.FromVersionLabel,
						row.ToVersionLabel,
						row.FromSpoilerFlag,
						row.ToSpoilerFlag,
						row.ChangeType,
					})
				}
				return writeCSV(cmd.OutOrStdout(), []string{
					"review_key",
					"work_href",
					"category",
					"review_type",
					"platform_key",
					"from_score",
					"to_score",
					"score_diff",
					"from_quote",
					"to_quote",
					"from_thumbs_up",
					"to_thumbs_up",
					"from_thumbs_down",
					"to_thumbs_down",
					"from_version_label",
					"to_version_label",
					"from_spoiler_flag",
					"to_spoiler_flag",
					"change_type",
				}, csvRows)
			default:
				writer := newTabWriter(cmd.OutOrStdout())
				_, _ = fmt.Fprintln(writer, "REVIEW_KEY\tWORK_HREF\tCATEGORY\tTYPE\tPLATFORM\tFROM_SCORE\tTO_SCORE\tSCORE_DIFF\tCHANGE_TYPE")
				for _, row := range mapped {
					_, _ = fmt.Fprintf(
						writer,
						"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
						row.ReviewKey,
						row.WorkHref,
						row.Category,
						row.ReviewType,
						row.PlatformKey,
						row.FromScore,
						row.ToScore,
						row.ScoreDiff,
						row.ChangeType,
					)
				}
				return writer.Flush()
			}
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&fromRunID, "from-run-id", "", "Source review crawl run id")
	cmd.Flags().StringVar(&toRunID, "to-run-id", "", "Target review crawl run id")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&reviewType, "review-type", "", "Optional review type filter: critic|user")
	cmd.Flags().StringVar(&platform, "platform", "", "Optional platform filter")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table|json|csv")
	cmd.Flags().BoolVar(&includeUnchanged, "include-unchanged", false, "Include unchanged rows in the comparison")
	addCheckpointFlag(cmd, &checkpoint)
	_ = cmd.MarkFlagRequired("from-run-id")
	_ = cmd.MarkFlagRequired("to-run-id")

	return cmd
}
