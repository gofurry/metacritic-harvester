package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newDetailCompareCommand() *cobra.Command {
	var (
		dbPath           string
		fromRunID        string
		toRunID          string
		category         string
		workHref         string
		format           string
		includeUnchanged bool
		checkpoint       bool
	)

	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare two detail crawl runs using snapshot rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("from_run_id", fromRunID),
				scopePart("to_run_id", toRunID),
				scopePart("category", category),
				scopePart("work_href", workHref),
				scopePart("format", format),
			)
			if err := validateOptionalCategoryMetric(category, ""); err != nil {
				return err
			}
			workHref = domain.NormalizeWorkHref(workHref, config.DefaultBaseURL)
			switch format {
			case "table", "json", "csv":
			default:
				return fmt.Errorf("format must be table, json, or csv")
			}

			repo, closeFn, err := openRepository(cmd.Context(), dbPath, checkpoint)
			if err != nil {
				return wrapReadCommandError("detail compare", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			rows, err := repo.CompareWorkDetails(cmd.Context(), storage.CompareWorkDetailsFilter{
				FromRunID:        fromRunID,
				ToRunID:          toRunID,
				Category:         category,
				WorkHref:         workHref,
				IncludeUnchanged: includeUnchanged,
			})
			if err != nil {
				return wrapReadCommandError("detail compare", err, scope)
			}

			mapped := mapDetailCompareRows(rows)
			switch format {
			case "json":
				return writeJSON(cmd.OutOrStdout(), mapped)
			case "csv":
				csvRows := make([][]string, 0, len(mapped))
				for _, row := range mapped {
					csvRows = append(csvRows, []string{
						row.WorkHref,
						row.Category,
						row.ChangeType,
						row.FromTitle,
						row.ToTitle,
						row.FromReleaseDate,
						row.ToReleaseDate,
						row.FromMetascore,
						row.ToMetascore,
						row.FromUserScore,
						row.ToUserScore,
						row.FromRating,
						row.ToRating,
						row.FromDuration,
						row.ToDuration,
						row.FromTagline,
						row.ToTagline,
						fmt.Sprintf("%t", row.DetailsJSONChanged),
						row.FromDetailsJSON,
						row.ToDetailsJSON,
					})
				}
				return writeCSV(cmd.OutOrStdout(), []string{
					"work_href",
					"category",
					"change_type",
					"from_title",
					"to_title",
					"from_release_date",
					"to_release_date",
					"from_metascore",
					"to_metascore",
					"from_user_score",
					"to_user_score",
					"from_rating",
					"to_rating",
					"from_duration",
					"to_duration",
					"from_tagline",
					"to_tagline",
					"details_json_changed",
					"from_details_json",
					"to_details_json",
				}, csvRows)
			default:
				writer := newTabWriter(cmd.OutOrStdout())
				_, _ = fmt.Fprintln(writer, "WORK_HREF\tCATEGORY\tCHANGE_TYPE\tFROM_TITLE\tTO_TITLE\tFROM_RELEASE_DATE\tTO_RELEASE_DATE\tFROM_METASCORE\tTO_METASCORE\tFROM_USER_SCORE\tTO_USER_SCORE\tFROM_RATING\tTO_RATING\tFROM_DURATION\tTO_DURATION\tFROM_TAGLINE\tTO_TAGLINE\tDETAILS_JSON_CHANGED")
				for _, row := range mapped {
					_, _ = fmt.Fprintf(
						writer,
						"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%t\n",
						row.WorkHref,
						row.Category,
						row.ChangeType,
						row.FromTitle,
						row.ToTitle,
						row.FromReleaseDate,
						row.ToReleaseDate,
						row.FromMetascore,
						row.ToMetascore,
						row.FromUserScore,
						row.ToUserScore,
						row.FromRating,
						row.ToRating,
						row.FromDuration,
						row.ToDuration,
						row.FromTagline,
						row.ToTagline,
						row.DetailsJSONChanged,
					)
				}
				return writer.Flush()
			}
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&fromRunID, "from-run-id", "", "Source detail crawl run id")
	cmd.Flags().StringVar(&toRunID, "to-run-id", "", "Target detail crawl run id")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table|json|csv")
	cmd.Flags().BoolVar(&includeUnchanged, "include-unchanged", false, "Include unchanged rows in the comparison")
	addCheckpointFlag(cmd, &checkpoint)
	_ = cmd.MarkFlagRequired("from-run-id")
	_ = cmd.MarkFlagRequired("to-run-id")

	return cmd
}
