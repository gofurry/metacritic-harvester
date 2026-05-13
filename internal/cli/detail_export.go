package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newDetailExportCommand() *cobra.Command {
	var (
		dbPath     string
		category   string
		workHref   string
		runID      string
		format     string
		profile    string
		output     string
		checkpoint bool
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export current work_details rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("category", category),
				scopePart("work_href", workHref),
				scopePart("run_id", runID),
				scopePart("profile", profile),
				scopePart("format", format),
			)
			if err := validateOptionalCategoryMetric(category, ""); err != nil {
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
				return wrapReadCommandError("detail export", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			filter := storage.ListWorkDetailsFilter{
				Category: category,
				WorkHref: workHref,
				Limit:    -1,
			}
			var records []detailExportRecord
			if runID == "" {
				rows, err := repo.ListWorkDetailsForExport(cmd.Context(), filter)
				if err != nil {
					return wrapReadCommandError("detail export", err, scope)
				}
				records, err = mapWorkDetailsForExport(rows)
				if err != nil {
					return wrapReadCommandError("detail export", err, scope)
				}
			} else {
				rows, err := repo.ListWorkDetailSnapshotsByRun(cmd.Context(), runID, filter)
				if err != nil {
					return wrapReadCommandError("detail export", err, scope)
				}
				records, err = mapWorkDetailSnapshots(rows)
				if err != nil {
					return wrapReadCommandError("detail export", err, scope)
				}
			}

			file, err := createOutputFile(output)
			if err != nil {
				return wrapReadCommandError("detail export", err, scope, scopePart("output", output))
			}
			defer file.Close()

			switch profile {
			case exportProfileSummary:
				summary := summarizeDetailExportRecords(records)
				if format == "json" {
					return writeJSON(file, summary)
				}
				csvRows := make([][]string, 0, len(summary))
				for _, row := range summary {
					csvRows = append(csvRows, []string{
						row.RunID,
						row.Category,
						fmt.Sprintf("%d", row.RowCount),
						fmt.Sprintf("%d", row.WithMetascoreCount),
						fmt.Sprintf("%d", row.WithUserScoreCount),
						fmt.Sprintf("%d", row.WithRatingCount),
						fmt.Sprintf("%d", row.WithDurationCount),
					})
				}
				return writeCSV(file, []string{
					"run_id",
					"category",
					"row_count",
					"with_metascore_count",
					"with_user_score_count",
					"with_rating_count",
					"with_duration_count",
				}, csvRows)
			case exportProfileFlat:
				flat := mapDetailExportRecordsToFlat(records)
				if format == "json" {
					return writeJSON(file, flat)
				}
				csvRows := make([][]string, 0, len(flat))
				for _, row := range flat {
					csvRows = append(csvRows, []string{
						row.RunID,
						row.WorkHref,
						row.Category,
						row.Title,
						row.ReleaseDate,
						row.Metascore,
						row.UserScore,
						row.Rating,
						row.Duration,
						row.Tagline,
						row.LastFetchedAt,
						row.GenresCSV,
						row.PlatformsCSV,
						row.DevelopersCSV,
						row.PublishersCSV,
						row.ProductionCompaniesCSV,
						row.DirectorsCSV,
						row.WritersCSV,
						fmt.Sprintf("%d", row.AwardsCount),
						fmt.Sprintf("%d", row.SeasonsCount),
					})
				}
				return writeCSV(file, []string{
					"run_id",
					"work_href",
					"category",
					"title",
					"release_date",
					"metascore",
					"user_score",
					"rating",
					"duration",
					"tagline",
					"last_fetched_at",
					"genres_csv",
					"platforms_csv",
					"developers_csv",
					"publishers_csv",
					"production_companies_csv",
					"directors_csv",
					"writers_csv",
					"awards_count",
					"seasons_count",
				}, csvRows)
			}

			mapped := mapDetailExportRecordsToRaw(records)
			if format == "json" {
				return writeJSON(file, mapped)
			}

			csvRows := make([][]string, 0, len(mapped))
			for _, row := range mapped {
				csvRows = append(csvRows, []string{
					row.WorkHref,
					row.Category,
					row.Title,
					row.ReleaseDate,
					row.Metascore,
					row.UserScore,
					row.Rating,
					row.Duration,
					row.Tagline,
					row.LastFetchedAt,
					row.RawDetailsJSON,
				})
			}
			return writeCSV(file, []string{
				"work_href",
				"category",
				"title",
				"release_date",
				"metascore",
				"user_score",
				"rating",
				"duration",
				"tagline",
				"last_fetched_at",
				"details_json",
			}, csvRows)
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().StringVar(&runID, "run-id", "", "Optional crawl run id; when set, export snapshot rows from work_detail_snapshots")
	cmd.Flags().StringVar(&format, "format", "csv", "Export format: csv|json")
	cmd.Flags().StringVar(&profile, "profile", exportProfileRaw, "Export profile: raw|flat|summary")
	cmd.Flags().StringVar(&output, "output", "", "Output file path")
	addCheckpointFlag(cmd, &checkpoint)
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
