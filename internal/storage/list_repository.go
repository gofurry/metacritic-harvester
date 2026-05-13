package storage

import (
	"context"
	"database/sql"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	sqlcgen "github.com/gofurry/metacritic-harvester/internal/storage/sqlcgen"
)

type ListRepository interface {
	UpsertWork(ctx context.Context, work domain.Work) error
	InsertListEntry(ctx context.Context, entry domain.ListEntry) error
	UpsertLatestListEntry(ctx context.Context, entry domain.ListEntry) error
	SaveListEntrySnapshot(ctx context.Context, work domain.Work, entry domain.ListEntry) error
	SaveListEntrySnapshots(ctx context.Context, snapshots []ListEntrySnapshot) error
	CreateCrawlRun(ctx context.Context, runID string, source string, taskName string, task domain.ListTask, startedAt time.Time) error
	CompleteCrawlRun(ctx context.Context, runID string, finishedAt time.Time) error
	FailCrawlRun(ctx context.Context, runID string, finishedAt time.Time, message string) error
}

type ListEntrySnapshot struct {
	Work  domain.Work
	Entry domain.ListEntry
}

type Repository struct {
	db      *sql.DB
	queries *sqlcgen.Queries
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{
		db:      db,
		queries: sqlcgen.New(db),
	}
}

func (r *Repository) UpsertWork(ctx context.Context, work domain.Work) error {
	return upsertWork(ctx, r.queries, work)
}

func upsertWork(ctx context.Context, queries *sqlcgen.Queries, work domain.Work) error {
	return queries.UpsertWork(ctx, sqlcgen.UpsertWorkParams{
		Href:        work.Href,
		Name:        work.Name,
		ImageUrl:    sql.NullString{String: work.ImageURL, Valid: work.ImageURL != ""},
		ReleaseDate: sql.NullString{String: work.ReleaseDate, Valid: work.ReleaseDate != ""},
		Category:    string(work.Category),
	})
}

func (r *Repository) InsertListEntry(ctx context.Context, entry domain.ListEntry) error {
	return insertListEntry(ctx, r.queries, entry)
}

func insertListEntry(ctx context.Context, queries *sqlcgen.Queries, entry domain.ListEntry) error {
	return queries.InsertListEntry(ctx, sqlcgen.InsertListEntryParams{
		CrawlRunID: entry.CrawlRunID,
		WorkHref:   entry.WorkHref,
		Category:   string(entry.Category),
		Metric:     string(entry.Metric),
		PageNo:     int64(entry.Page),
		RankNo:     int64(entry.Rank),
		Metascore:  sql.NullString{String: entry.Metascore, Valid: entry.Metascore != ""},
		UserScore:  sql.NullString{String: entry.UserScore, Valid: entry.UserScore != ""},
		FilterKey:  entry.FilterKey,
		CrawledAt:  entry.CrawledAt.Format(time.RFC3339),
	})
}

func (r *Repository) UpsertLatestListEntry(ctx context.Context, entry domain.ListEntry) error {
	return upsertLatestListEntry(ctx, r.queries, entry)
}

func upsertLatestListEntry(ctx context.Context, queries *sqlcgen.Queries, entry domain.ListEntry) error {
	return queries.UpsertLatestListEntry(ctx, sqlcgen.UpsertLatestListEntryParams{
		WorkHref:         entry.WorkHref,
		Category:         string(entry.Category),
		Metric:           string(entry.Metric),
		FilterKey:        entry.FilterKey,
		PageNo:           int64(entry.Page),
		RankNo:           int64(entry.Rank),
		Metascore:        sql.NullString{String: entry.Metascore, Valid: entry.Metascore != ""},
		UserScore:        sql.NullString{String: entry.UserScore, Valid: entry.UserScore != ""},
		SourceCrawlRunID: entry.CrawlRunID,
		LastCrawledAt:    entry.CrawledAt.Format(time.RFC3339),
	})
}

func (r *Repository) SaveListEntrySnapshot(ctx context.Context, work domain.Work, entry domain.ListEntry) error {
	return r.SaveListEntrySnapshots(ctx, []ListEntrySnapshot{{Work: work, Entry: entry}})
}

func (r *Repository) SaveListEntrySnapshots(ctx context.Context, snapshots []ListEntrySnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	queries := r.queries.WithTx(tx)

	for _, snapshot := range snapshots {
		if err := upsertWork(ctx, queries, snapshot.Work); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := insertListEntry(ctx, queries, snapshot.Entry); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := upsertLatestListEntry(ctx, queries, snapshot.Entry); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (r *Repository) CreateCrawlRun(ctx context.Context, runID string, source string, taskName string, task domain.ListTask, startedAt time.Time) error {
	return r.queries.CreateCrawlRun(ctx, sqlcgen.CreateCrawlRunParams{
		RunID:        runID,
		Source:       source,
		TaskName:     taskName,
		Category:     string(task.Category),
		Metric:       string(task.Metric),
		FilterKey:    task.Filter.Key(),
		StartedAt:    startedAt.Format(time.RFC3339),
		Status:       "running",
		ErrorMessage: sql.NullString{},
	})
}

func (r *Repository) CompleteCrawlRun(ctx context.Context, runID string, finishedAt time.Time) error {
	return r.queries.CompleteCrawlRun(ctx, sqlcgen.CompleteCrawlRunParams{
		FinishedAt: sql.NullString{String: finishedAt.Format(time.RFC3339), Valid: true},
		Status:     "completed",
		RunID:      runID,
	})
}

func (r *Repository) FailCrawlRun(ctx context.Context, runID string, finishedAt time.Time, message string) error {
	return r.queries.FailCrawlRun(ctx, sqlcgen.FailCrawlRunParams{
		FinishedAt:   sql.NullString{String: finishedAt.Format(time.RFC3339), Valid: true},
		Status:       "failed",
		ErrorMessage: sql.NullString{String: message, Valid: message != ""},
		RunID:        runID,
	})
}

func (r *Repository) GetWorkByHref(ctx context.Context, href string) (sqlcgen.Work, error) {
	return r.queries.GetWorkByHref(ctx, href)
}

func (r *Repository) GetLatestListEntry(ctx context.Context, entry domain.ListEntry) (sqlcgen.LatestListEntry, error) {
	return r.queries.GetLatestListEntry(ctx, sqlcgen.GetLatestListEntryParams{
		WorkHref:  entry.WorkHref,
		Category:  string(entry.Category),
		Metric:    string(entry.Metric),
		FilterKey: entry.FilterKey,
	})
}

func (r *Repository) CountWorks(ctx context.Context) (int64, error) {
	return r.queries.CountWorks(ctx)
}

func (r *Repository) CountListEntries(ctx context.Context) (int64, error) {
	return r.queries.CountListEntries(ctx)
}

func (r *Repository) CountLatestListEntries(ctx context.Context) (int64, error) {
	return r.queries.CountLatestListEntries(ctx)
}

func (r *Repository) CountCrawlRuns(ctx context.Context) (int64, error) {
	return r.queries.CountCrawlRuns(ctx)
}

func (r *Repository) GetCrawlRun(ctx context.Context, runID string) (sqlcgen.CrawlRun, error) {
	return r.queries.GetCrawlRun(ctx, runID)
}

func (r *Repository) ListCrawlRuns(ctx context.Context, limit int) ([]sqlcgen.CrawlRun, error) {
	return r.queries.ListCrawlRuns(ctx, positiveLimit(limit))
}

type ListLatestEntriesFilter struct {
	Category  string
	Metric    string
	WorkHref  string
	FilterKey string
	Limit     int
}

func (r *Repository) ListLatestEntries(ctx context.Context, filter ListLatestEntriesFilter) ([]sqlcgen.LatestListEntry, error) {
	return r.queries.ListLatestEntries(ctx, sqlcgen.ListLatestEntriesParams{
		Category:  emptyString(filter.Category),
		Metric:    emptyString(filter.Metric),
		WorkHref:  emptyString(filter.WorkHref),
		FilterKey: emptyString(filter.FilterKey),
		LimitRows: positiveLimit(filter.Limit),
	})
}

func (r *Repository) ListListEntriesByRun(ctx context.Context, runID string, filter ListLatestEntriesFilter) ([]sqlcgen.ListEntry, error) {
	return r.queries.ListListEntriesByRun(ctx, sqlcgen.ListListEntriesByRunParams{
		CrawlRunID: runID,
		Category:   emptyString(filter.Category),
		Metric:     emptyString(filter.Metric),
		WorkHref:   emptyString(filter.WorkHref),
		FilterKey:  emptyString(filter.FilterKey),
		LimitRows:  positiveLimit(filter.Limit),
	})
}

func (r *Repository) GetLatestEntryByWork(ctx context.Context, workHref string, category string, metric string) ([]sqlcgen.LatestListEntry, error) {
	return r.queries.GetLatestEntryByWork(ctx, sqlcgen.GetLatestEntryByWorkParams{
		WorkHref: workHref,
		Category: emptyString(category),
		Metric:   emptyString(metric),
	})
}

type CompareRunsFilter struct {
	FromRunID        string
	ToRunID          string
	Category         string
	Metric           string
	IncludeUnchanged bool
}

func (r *Repository) CompareCrawlRuns(ctx context.Context, filter CompareRunsFilter) ([]sqlcgen.CompareCrawlRunsRow, error) {
	return r.queries.CompareCrawlRuns(ctx, sqlcgen.CompareCrawlRunsParams{
		IncludeUnchanged: boolAsInt(filter.IncludeUnchanged),
		FromRunID:        filter.FromRunID,
		Category:         emptyString(filter.Category),
		Metric:           emptyString(filter.Metric),
		ToRunID:          filter.ToRunID,
	})
}

func emptyString(value string) string {
	return value
}

func positiveLimit(limit int) int {
	if limit <= 0 {
		return -1
	}
	return limit
}

func boolAsInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
