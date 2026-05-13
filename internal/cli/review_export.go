package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newReviewExportCommand() *cobra.Command {
	var (
		dbPath     string
		category   string
		reviewType string
		platform   string
		workHref   string
		runID      string
		format     string
		profile    string
		output     string
		checkpoint bool
	)

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export current latest_reviews rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("category", category),
				scopePart("review_type", reviewType),
				scopePart("platform", platform),
				scopePart("work_href", workHref),
				scopePart("run_id", runID),
				scopePart("profile", profile),
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
				return wrapReadCommandError("review export", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			filter := storage.ListLatestReviewsFilter{
				Category:   category,
				ReviewType: reviewType,
				Platform:   platform,
				WorkHref:   workHref,
				Limit:      -1,
			}
			var records []reviewExportRecord
			if runID == "" {
				rows, err := repo.ListLatestReviews(cmd.Context(), filter)
				if err != nil {
					return wrapReadCommandError("review export", err, scope)
				}
				records = mapLatestReviewsForExport(rows)
			} else {
				rows, err := repo.ListReviewSnapshotsByRun(cmd.Context(), runID, filter)
				if err != nil {
					return wrapReadCommandError("review export", err, scope)
				}
				records = mapReviewSnapshotsForExport(rows)
			}
			file, err := createOutputFile(output)
			if err != nil {
				return wrapReadCommandError("review export", err, scope, scopePart("output", output))
			}
			defer file.Close()

			switch profile {
			case exportProfileSummary:
				summary := summarizeReviewExportRecords(records)
				if format == "json" {
					return writeJSON(file, summary)
				}
				csvRows := make([][]string, 0, len(summary))
				for _, row := range summary {
					csvRows = append(csvRows, []string{
						row.RunID,
						row.Category,
						row.ReviewType,
						row.PlatformKey,
						fmt.Sprintf("%d", row.RowCount),
						fmt.Sprintf("%d", row.ScoredCount),
						fmt.Sprintf("%g", row.AvgScore),
						fmt.Sprintf("%d", row.WithQuoteCount),
						fmt.Sprintf("%d", row.WithPublicationCount),
						fmt.Sprintf("%d", row.WithUsernameCount),
					})
				}
				return writeCSV(file, []string{
					"run_id",
					"category",
					"review_type",
					"platform_key",
					"row_count",
					"scored_count",
					"avg_score",
					"with_quote_count",
					"with_publication_count",
					"with_username_count",
				}, csvRows)
			case exportProfileFlat:
				flat := mapReviewExportRecordsToFlat(records)
				if format == "json" {
					return writeJSON(file, flat)
				}
				csvRows := make([][]string, 0, len(flat))
				for _, row := range flat {
					csvRows = append(csvRows, []string{
						row.RunID,
						row.ReviewKey,
						row.ExternalReviewID,
						row.WorkHref,
						row.Category,
						row.ReviewType,
						row.PlatformKey,
						row.ReviewURL,
						row.ReviewDate,
						row.Score,
						row.Quote,
						row.PublicationName,
						row.PublicationSlug,
						row.AuthorName,
						row.AuthorSlug,
						row.SeasonLabel,
						row.Username,
						row.UserSlug,
						row.ThumbsUp,
						row.ThumbsDown,
						row.VersionLabel,
						row.SpoilerFlag,
						row.LastCrawledAt,
					})
				}
				return writeCSV(file, []string{
					"run_id",
					"review_key",
					"external_review_id",
					"work_href",
					"category",
					"review_type",
					"platform_key",
					"review_url",
					"review_date",
					"score",
					"quote",
					"publication_name",
					"publication_slug",
					"author_name",
					"author_slug",
					"season_label",
					"username",
					"user_slug",
					"thumbs_up",
					"thumbs_down",
					"version_label",
					"spoiler_flag",
					"last_crawled_at",
				}, csvRows)
			}

			mapped := mapReviewExportRecordsToRaw(records)
			if format == "json" {
				return writeJSON(file, mapped)
			}

			csvRows := make([][]string, 0, len(mapped))
			for _, row := range mapped {
				csvRows = append(csvRows, []string{
					row.ReviewKey,
					row.ExternalReviewID,
					row.WorkHref,
					row.Category,
					row.ReviewType,
					row.PlatformKey,
					row.ReviewURL,
					row.ReviewDate,
					row.Score,
					row.Quote,
					row.PublicationName,
					row.PublicationSlug,
					row.AuthorName,
					row.AuthorSlug,
					row.SeasonLabel,
					row.Username,
					row.UserSlug,
					row.ThumbsUp,
					row.ThumbsDown,
					row.VersionLabel,
					row.SpoilerFlag,
					row.SourcePayloadJSON,
					row.SourceCrawlRunID,
					row.LastCrawledAt,
				})
			}
			return writeCSV(file, []string{
				"review_key",
				"external_review_id",
				"work_href",
				"category",
				"review_type",
				"platform_key",
				"review_url",
				"review_date",
				"score",
				"quote",
				"publication_name",
				"publication_slug",
				"author_name",
				"author_slug",
				"season_label",
				"username",
				"user_slug",
				"thumbs_up",
				"thumbs_down",
				"version_label",
				"spoiler_flag",
				"source_payload_json",
				"source_crawl_run_id",
				"last_crawled_at",
			}, csvRows)
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&reviewType, "review-type", "", "Optional review type filter: critic|user")
	cmd.Flags().StringVar(&platform, "platform", "", "Optional platform filter")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().StringVar(&runID, "run-id", "", "Optional crawl run id; when set, export snapshot rows from review_snapshots")
	cmd.Flags().StringVar(&format, "format", "csv", "Export format: csv|json")
	cmd.Flags().StringVar(&profile, "profile", exportProfileRaw, "Export profile: raw|flat|summary")
	cmd.Flags().StringVar(&output, "output", "", "Output file path")
	addCheckpointFlag(cmd, &checkpoint)
	_ = cmd.MarkFlagRequired("output")

	return cmd
}
