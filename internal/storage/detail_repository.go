package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	sqlcgen "github.com/gofurry/metacritic-harvester/internal/storage/sqlcgen"
)

const (
	DetailFetchStatusRunning   = "running"
	DetailFetchStatusSucceeded = "succeeded"
	DetailFetchStatusFailed    = "failed"
)

type DetailCandidate struct {
	Work            domain.Work
	FetchStatus     string
	LastAttemptedAt string
	LastFetchedAt   string
	LastRunID       string
	LastError       string
	LastErrorType   string
	LastErrorStage  string
	UpdatedAt       string
	HasDetail       bool
}

type ListDetailCandidatesFilter struct {
	Category string
	WorkHref string
	Limit    int
	Force    bool
}

type ListWorkDetailsFilter struct {
	Category string
	WorkHref string
	Limit    int
}

type CompareWorkDetailsFilter struct {
	FromRunID        string
	ToRunID          string
	Category         string
	WorkHref         string
	IncludeUnchanged bool
}

type DetailFetchFailure struct {
	Message    string
	ErrorType  string
	ErrorStage string
}

func (r *Repository) ListDetailCandidates(ctx context.Context, filter ListDetailCandidatesFilter) ([]DetailCandidate, error) {
	rows, err := r.queries.ListDetailCandidates(ctx, sqlcgen.ListDetailCandidatesParams{
		Category:     emptyString(filter.Category),
		WorkHref:     emptyString(filter.WorkHref),
		ForceRefresh: boolAsInt(filter.Force),
		LimitRows:    positiveLimit(filter.Limit),
	})
	if err != nil {
		return nil, err
	}

	result := make([]DetailCandidate, 0, len(rows))
	for _, row := range rows {
		result = append(result, DetailCandidate{
			Work: domain.Work{
				Name:        row.Name,
				Href:        row.Href,
				ImageURL:    row.ImageUrl.String,
				ReleaseDate: row.ReleaseDate.String,
				Category:    domain.Category(row.Category),
			},
			FetchStatus:     row.FetchStatus.String,
			LastAttemptedAt: row.LastAttemptedAt.String,
			LastFetchedAt:   row.LastFetchedAt.String,
			LastRunID:       row.LastRunID.String,
			LastError:       row.LastError.String,
			LastErrorType:   row.LastErrorType.String,
			LastErrorStage:  row.LastErrorStage.String,
			UpdatedAt:       row.FetchUpdatedAt.String,
			HasDetail:       row.HasDetail,
		})
	}
	return result, nil
}

func (r *Repository) SaveWorkDetail(ctx context.Context, detail domain.WorkDetail, attemptedAt time.Time, runID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	queries := r.queries.WithTx(tx)

	if err := updateWorkFromDetail(ctx, queries, detail); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := insertWorkDetailSnapshot(ctx, queries, detail, runID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := upsertWorkDetail(ctx, queries, detail); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := upsertDetailFetchStateSucceeded(ctx, queries, detail.WorkHref, attemptedAt, detail.LastFetchedAt, runID); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (r *Repository) CreateDetailCrawlRun(ctx context.Context, runID string, category string, taskName string, filterKey string, startedAt time.Time) error {
	return r.queries.CreateCrawlRun(ctx, sqlcgen.CreateCrawlRunParams{
		RunID:        runID,
		Source:       "crawl detail",
		TaskName:     taskName,
		Category:     category,
		Metric:       "detail",
		FilterKey:    filterKey,
		StartedAt:    startedAt.Format(time.RFC3339),
		Status:       "running",
		ErrorMessage: sql.NullString{},
	})
}

func (r *Repository) CompleteDetailCrawlRun(ctx context.Context, runID string, finishedAt time.Time) error {
	return r.queries.CompleteCrawlRun(ctx, sqlcgen.CompleteCrawlRunParams{
		FinishedAt: sql.NullString{String: finishedAt.Format(time.RFC3339), Valid: true},
		Status:     "completed",
		RunID:      runID,
	})
}

func (r *Repository) FailDetailCrawlRun(ctx context.Context, runID string, finishedAt time.Time, message string) error {
	return r.queries.FailCrawlRun(ctx, sqlcgen.FailCrawlRunParams{
		FinishedAt:   sql.NullString{String: finishedAt.Format(time.RFC3339), Valid: true},
		Status:       "failed",
		ErrorMessage: sql.NullString{String: message, Valid: message != ""},
		RunID:        runID,
	})
}

func (r *Repository) RecoverStaleDetailFetchStates(ctx context.Context, filter ListDetailCandidatesFilter, staleBefore time.Time, runID string) (int64, error) {
	now := time.Now().UTC()
	return r.queries.RecoverStaleDetailFetchStates(ctx, sqlcgen.RecoverStaleDetailFetchStatesParams{
		LastRunID:   nullableString(runID),
		UpdatedAt:   now.Format(time.RFC3339),
		StaleBefore: nullableTime(staleBefore),
		Category:    emptyString(filter.Category),
		WorkHref:    emptyString(filter.WorkHref),
	})
}

func (r *Repository) MarkDetailRunning(ctx context.Context, workHref string, attemptedAt time.Time, runID string) error {
	return upsertDetailFetchStateRunning(ctx, r.queries, workHref, attemptedAt, runID)
}

func (r *Repository) MarkDetailFailed(ctx context.Context, workHref string, attemptedAt time.Time, runID string, failure DetailFetchFailure) error {
	return upsertDetailFetchStateFailed(ctx, r.queries, workHref, attemptedAt, runID, failure)
}

func (r *Repository) GetWorkDetail(ctx context.Context, workHref string) (sqlcgen.WorkDetail, error) {
	return r.queries.GetWorkDetail(ctx, workHref)
}

func (r *Repository) ListWorkDetails(ctx context.Context, filter ListWorkDetailsFilter) ([]sqlcgen.WorkDetail, error) {
	return r.queries.ListWorkDetails(ctx, sqlcgen.ListWorkDetailsParams{
		Category:  emptyString(filter.Category),
		WorkHref:  emptyString(filter.WorkHref),
		LimitRows: positiveLimit(filter.Limit),
	})
}

func (r *Repository) ListWorkDetailsForExport(ctx context.Context, filter ListWorkDetailsFilter) ([]sqlcgen.ListWorkDetailsForExportRow, error) {
	return r.queries.ListWorkDetailsForExport(ctx, sqlcgen.ListWorkDetailsForExportParams{
		Category:  emptyString(filter.Category),
		WorkHref:  emptyString(filter.WorkHref),
		LimitRows: positiveLimit(filter.Limit),
	})
}

func (r *Repository) ListWorkDetailSnapshotsByRun(ctx context.Context, runID string, filter ListWorkDetailsFilter) ([]sqlcgen.WorkDetailSnapshot, error) {
	return r.queries.ListWorkDetailSnapshotsByRun(ctx, sqlcgen.ListWorkDetailSnapshotsByRunParams{
		CrawlRunID: runID,
		Category:   emptyString(filter.Category),
		WorkHref:   emptyString(filter.WorkHref),
		LimitRows:  positiveLimit(filter.Limit),
	})
}

func (r *Repository) CompareWorkDetails(ctx context.Context, filter CompareWorkDetailsFilter) ([]sqlcgen.CompareWorkDetailSnapshotsRow, error) {
	return r.queries.CompareWorkDetailSnapshots(ctx, sqlcgen.CompareWorkDetailSnapshotsParams{
		IncludeUnchanged: boolAsInt(filter.IncludeUnchanged),
		FromRunID:        filter.FromRunID,
		ToRunID:          filter.ToRunID,
		Category:         emptyString(filter.Category),
		WorkHref:         emptyString(filter.WorkHref),
	})
}

func (r *Repository) GetDetailFetchState(ctx context.Context, workHref string) (sqlcgen.DetailFetchState, error) {
	return r.queries.GetDetailFetchState(ctx, workHref)
}

func (r *Repository) CountWorkDetails(ctx context.Context) (int64, error) {
	return r.queries.CountWorkDetails(ctx)
}

func (r *Repository) CountWorkDetailSnapshots(ctx context.Context) (int64, error) {
	return r.queries.CountWorkDetailSnapshots(ctx)
}

func (r *Repository) CountWorkDetailSnapshotsByWorkHref(ctx context.Context, workHref string) (int64, error) {
	return r.queries.CountWorkDetailSnapshotsByWorkHref(ctx, workHref)
}

func updateWorkFromDetail(ctx context.Context, queries *sqlcgen.Queries, detail domain.WorkDetail) error {
	return queries.UpdateWorkFromDetail(ctx, sqlcgen.UpdateWorkFromDetailParams{
		Name:        detail.Title,
		ReleaseDate: detail.ReleaseDate,
		Category:    string(detail.Category),
		Href:        detail.WorkHref,
	})
}

func insertWorkDetailSnapshot(ctx context.Context, queries *sqlcgen.Queries, detail domain.WorkDetail, runID string) error {
	detailsJSON, err := marshalWorkDetailExtras(detail)
	if err != nil {
		return err
	}

	return queries.InsertWorkDetailSnapshot(ctx, sqlcgen.InsertWorkDetailSnapshotParams{
		WorkHref:             detail.WorkHref,
		CrawlRunID:           runID,
		Category:             string(detail.Category),
		Title:                detail.Title,
		Summary:              nullableString(detail.Summary),
		ReleaseDate:          nullableString(detail.ReleaseDate),
		Metascore:            nullableString(detail.Metascore),
		MetascoreSentiment:   nullableString(detail.MetascoreSentiment),
		MetascoreReviewCount: nullableInt(detail.MetascoreReviewCount),
		UserScore:            nullableString(detail.UserScore),
		UserScoreSentiment:   nullableString(detail.UserScoreSentiment),
		UserScoreCount:       nullableInt(detail.UserScoreCount),
		Rating:               nullableString(detail.Rating),
		Duration:             nullableString(detail.Duration),
		Tagline:              nullableString(detail.Tagline),
		DetailsJson:          string(detailsJSON),
		FetchedAt:            detail.LastFetchedAt.Format(time.RFC3339),
	})
}

func upsertWorkDetail(ctx context.Context, queries *sqlcgen.Queries, detail domain.WorkDetail) error {
	detailsJSON, err := marshalWorkDetailExtras(detail)
	if err != nil {
		return err
	}

	return queries.UpsertWorkDetail(ctx, sqlcgen.UpsertWorkDetailParams{
		WorkHref:             detail.WorkHref,
		Category:             string(detail.Category),
		Title:                detail.Title,
		Summary:              nullableString(detail.Summary),
		ReleaseDate:          nullableString(detail.ReleaseDate),
		Metascore:            nullableString(detail.Metascore),
		MetascoreSentiment:   nullableString(detail.MetascoreSentiment),
		MetascoreReviewCount: nullableInt(detail.MetascoreReviewCount),
		UserScore:            nullableString(detail.UserScore),
		UserScoreSentiment:   nullableString(detail.UserScoreSentiment),
		UserScoreCount:       nullableInt(detail.UserScoreCount),
		Rating:               nullableString(detail.Rating),
		Duration:             nullableString(detail.Duration),
		Tagline:              nullableString(detail.Tagline),
		DetailsJson:          string(detailsJSON),
		LastFetchedAt:        detail.LastFetchedAt.Format(time.RFC3339),
	})
}

func marshalWorkDetailExtras(detail domain.WorkDetail) ([]byte, error) {
	detailsJSON, err := json.Marshal(detail.Details)
	if err != nil {
		return nil, err
	}
	if string(detailsJSON) == "null" {
		detailsJSON = []byte("{}")
	}
	return detailsJSON, nil
}

func upsertDetailFetchStateRunning(ctx context.Context, queries *sqlcgen.Queries, workHref string, attemptedAt time.Time, runID string) error {
	now := time.Now().UTC()
	return queries.UpsertDetailFetchStateRunning(ctx, sqlcgen.UpsertDetailFetchStateRunningParams{
		WorkHref:        workHref,
		LastAttemptedAt: nullableTime(attemptedAt),
		LastRunID:       nullableString(runID),
		UpdatedAt:       now.Format(time.RFC3339),
	})
}

func upsertDetailFetchStateSucceeded(ctx context.Context, queries *sqlcgen.Queries, workHref string, attemptedAt time.Time, fetchedAt time.Time, runID string) error {
	now := time.Now().UTC()
	return queries.UpsertDetailFetchStateSucceeded(ctx, sqlcgen.UpsertDetailFetchStateSucceededParams{
		WorkHref:        workHref,
		LastAttemptedAt: nullableTime(attemptedAt),
		LastFetchedAt:   nullableTime(fetchedAt),
		LastRunID:       nullableString(runID),
		UpdatedAt:       now.Format(time.RFC3339),
	})
}

func upsertDetailFetchStateFailed(ctx context.Context, queries *sqlcgen.Queries, workHref string, attemptedAt time.Time, runID string, failure DetailFetchFailure) error {
	if strings.TrimSpace(failure.Message) == "" {
		failure.Message = "detail processing failed"
	}
	now := time.Now().UTC()
	return queries.UpsertDetailFetchStateFailed(ctx, sqlcgen.UpsertDetailFetchStateFailedParams{
		WorkHref:        workHref,
		LastAttemptedAt: nullableTime(attemptedAt),
		LastRunID:       nullableString(runID),
		LastError:       nullableString(failure.Message),
		LastErrorType:   nullableString(failure.ErrorType),
		LastErrorStage:  nullableString(failure.ErrorStage),
		UpdatedAt:       now.Format(time.RFC3339),
	})
}

func nullableString(value string) sql.NullString {
	return sql.NullString{String: value, Valid: value != ""}
}

func nullableInt(value int) sql.NullInt64 {
	return sql.NullInt64{Int64: int64(value), Valid: value > 0}
}

func nullableTime(value time.Time) sql.NullString {
	if value.IsZero() {
		return sql.NullString{}
	}
	return sql.NullString{String: value.UTC().Format(time.RFC3339), Valid: true}
}
