package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func newDetailQueryCommand() *cobra.Command {
	var (
		dbPath     string
		category   string
		workHref   string
		limit      int
		format     string
		checkpoint bool
	)

	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query current work_details rows",
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			scope := buildReadScope(
				scopePart("db", dbPath),
				scopePart("category", category),
				scopePart("work_href", workHref),
				scopePartInt("limit", limit),
			)
			if err := validateOptionalCategoryMetric(category, ""); err != nil {
				return err
			}
			workHref = domain.NormalizeWorkHref(workHref, config.DefaultBaseURL)
			if format != "table" && format != "json" {
				return fmt.Errorf("format must be table or json")
			}

			repo, closeFn, err := openRepository(cmd.Context(), dbPath, checkpoint)
			if err != nil {
				return wrapReadCommandError("detail query", err, scope)
			}
			defer func() {
				err = finishReadRepository(err, closeFn)
			}()

			rows, err := repo.ListWorkDetails(cmd.Context(), storage.ListWorkDetailsFilter{
				Category: category,
				WorkHref: workHref,
				Limit:    limit,
			})
			if err != nil {
				return wrapReadCommandError("detail query", err, scope)
			}

			mapped, err := mapWorkDetails(rows)
			if err != nil {
				return wrapReadCommandError("detail query", err, scope)
			}
			if format == "json" {
				return writeJSON(cmd.OutOrStdout(), mapped)
			}

			writer := newTabWriter(cmd.OutOrStdout())
			_, _ = fmt.Fprintln(writer, "WORK_HREF\tCATEGORY\tTITLE\tRELEASE_DATE\tMETASCORE\tUSER_SCORE\tRATING\tDURATION\tTAGLINE\tLAST_FETCHED_AT")
			for _, row := range mapped {
				_, _ = fmt.Fprintf(
					writer,
					"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
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
				)
			}
			return writer.Flush()
		},
	}

	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().StringVar(&category, "category", "", "Optional category filter: game|movie|tv")
	cmd.Flags().StringVar(&workHref, "work-href", "", "Optional work href filter")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum number of rows to return")
	cmd.Flags().StringVar(&format, "format", "table", "Output format: table|json")
	addCheckpointFlag(cmd, &checkpoint)

	return cmd
}
