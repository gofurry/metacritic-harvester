package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	sqlcgen "github.com/gofurry/metacritic-harvester/internal/storage/sqlcgen"
)

const (
	ReviewFetchStatusRunning   = "running"
	ReviewFetchStatusSucceeded = "succeeded"
	ReviewFetchStatusFailed    = "failed"
)

type ListReviewCandidatesFilter struct {
	Category string
	WorkHref string
	Limit    int
}

type ListLatestReviewsFilter struct {
	Category   string
	ReviewType string
	Platform   string
	WorkHref   string
	Limit      int
}

type CompareReviewsFilter struct {
	FromRunID        string
	ToRunID          string
	Category         string
	ReviewType       string
	Platform         string
	WorkHref         string
	IncludeUnchanged bool
}

type ReviewFetchFailure struct {
	Message    string
	ErrorType  string
	ErrorStage string
}

func (r *Repository) ListReviewCandidates(ctx context.Context, filter ListReviewCandidatesFilter) ([]domain.Work, error) {
	rows, err := r.queries.ListReviewCandidateWorks(ctx, sqlcgen.ListReviewCandidateWorksParams{
		Category:  emptyString(filter.Category),
		WorkHref:  emptyString(filter.WorkHref),
		LimitRows: positiveLimit(filter.Limit),
	})
	if err != nil {
		return nil, err
	}

	result := make([]domain.Work, 0, len(rows))
	for _, row := range rows {
		result = append(result, domain.Work{
			Name:        row.Name,
			Href:        row.Href,
			ImageURL:    row.ImageUrl.String,
			ReleaseDate: row.ReleaseDate.String,
			Category:    domain.Category(row.Category),
		})
	}
	return result, nil
}

func (r *Repository) CreateReviewCrawlRun(ctx context.Context, runID string, source string, taskName string, category string, filterKey string, startedAt time.Time) error {
	if strings.TrimSpace(source) == "" {
		source = "crawl reviews"
	}
	return r.queries.CreateCrawlRun(ctx, sqlcgen.CreateCrawlRunParams{
		RunID:        runID,
		Source:       source,
		TaskName:     taskName,
		Category:     category,
		Metric:       "reviews",
		FilterKey:    filterKey,
		StartedAt:    startedAt.Format(time.RFC3339),
		Status:       "running",
		ErrorMessage: sql.NullString{},
	})
}

func (r *Repository) CompleteReviewCrawlRun(ctx context.Context, runID string, finishedAt time.Time) error {
	return r.queries.CompleteCrawlRun(ctx, sqlcgen.CompleteCrawlRunParams{
		FinishedAt: sql.NullString{String: finishedAt.Format(time.RFC3339), Valid: true},
		Status:     "completed",
		RunID:      runID,
	})
}

func (r *Repository) FailReviewCrawlRun(ctx context.Context, runID string, finishedAt time.Time, message string) error {
	return r.queries.FailCrawlRun(ctx, sqlcgen.FailCrawlRunParams{
		FinishedAt:   sql.NullString{String: finishedAt.Format(time.RFC3339), Valid: true},
		Status:       "failed",
		ErrorMessage: sql.NullString{String: message, Valid: message != ""},
		RunID:        runID,
	})
}

func (r *Repository) SaveReviewRecords(ctx context.Context, records []domain.ReviewRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	queries := r.queries.WithTx(tx)

	for _, record := range records {
		if err := upsertLatestReview(ctx, queries, record); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := insertReviewSnapshot(ctx, queries, record); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (r *Repository) MarkReviewRunning(ctx context.Context, scope domain.ReviewScope, attemptedAt time.Time, runID string) error {
	return upsertReviewFetchStateRunning(ctx, r.queries, scope, attemptedAt, runID)
}

func (r *Repository) MarkReviewSucceeded(ctx context.Context, scope domain.ReviewScope, attemptedAt time.Time, fetchedAt time.Time, runID string) error {
	return upsertReviewFetchStateSucceeded(ctx, r.queries, scope, attemptedAt, fetchedAt, runID)
}

func (r *Repository) MarkReviewFailed(ctx context.Context, scope domain.ReviewScope, attemptedAt time.Time, runID string, failure ReviewFetchFailure) error {
	return upsertReviewFetchStateFailed(ctx, r.queries, scope, attemptedAt, runID, failure)
}

func (r *Repository) GetReviewFetchState(ctx context.Context, scope domain.ReviewScope) (sqlcgen.ReviewFetchState, error) {
	return r.queries.GetReviewFetchState(ctx, sqlcgen.GetReviewFetchStateParams{
		WorkHref:    scope.WorkHref,
		ReviewType:  string(scope.ReviewType),
		PlatformKey: strings.TrimSpace(scope.PlatformKey),
	})
}

func (r *Repository) ListLatestReviews(ctx context.Context, filter ListLatestReviewsFilter) ([]sqlcgen.LatestReview, error) {
	return r.queries.ListLatestReviews(ctx, sqlcgen.ListLatestReviewsParams{
		Category:    emptyString(filter.Category),
		ReviewType:  emptyString(filter.ReviewType),
		PlatformKey: emptyString(filter.Platform),
		WorkHref:    emptyString(filter.WorkHref),
		LimitRows:   positiveLimit(filter.Limit),
	})
}

func (r *Repository) ListReviewSnapshotsByRun(ctx context.Context, runID string, filter ListLatestReviewsFilter) ([]sqlcgen.ReviewSnapshot, error) {
	return r.queries.ListReviewSnapshotsByRun(ctx, sqlcgen.ListReviewSnapshotsByRunParams{
		CrawlRunID:  runID,
		Category:    emptyString(filter.Category),
		ReviewType:  emptyString(filter.ReviewType),
		PlatformKey: emptyString(filter.Platform),
		WorkHref:    emptyString(filter.WorkHref),
		LimitRows:   positiveLimit(filter.Limit),
	})
}

func (r *Repository) CompareReviewSnapshots(ctx context.Context, filter CompareReviewsFilter) ([]sqlcgen.CompareReviewSnapshotsRow, error) {
	return r.queries.CompareReviewSnapshots(ctx, sqlcgen.CompareReviewSnapshotsParams{
		IncludeUnchanged: boolAsInt(filter.IncludeUnchanged),
		FromRunID:        filter.FromRunID,
		ToRunID:          filter.ToRunID,
		Category:         emptyString(filter.Category),
		ReviewType:       emptyString(filter.ReviewType),
		PlatformKey:      emptyString(filter.Platform),
		WorkHref:         emptyString(filter.WorkHref),
	})
}

func (r *Repository) CountLatestReviews(ctx context.Context) (int64, error) {
	return r.queries.CountLatestReviews(ctx)
}

func (r *Repository) CountReviewSnapshots(ctx context.Context) (int64, error) {
	return r.queries.CountReviewSnapshots(ctx)
}

func upsertLatestReview(ctx context.Context, queries *sqlcgen.Queries, record domain.ReviewRecord) error {
	payload := normalizeSourcePayload(record.SourcePayloadJSON)
	return queries.UpsertLatestReview(ctx, sqlcgen.UpsertLatestReviewParams{
		ReviewKey:         record.ReviewKey,
		ExternalReviewID:  nullableString(record.ExternalReviewID),
		WorkHref:          record.WorkHref,
		Category:          string(record.Category),
		ReviewType:        string(record.ReviewType),
		PlatformKey:       strings.TrimSpace(record.PlatformKey),
		ReviewUrl:         nullableString(record.ReviewURL),
		ReviewDate:        nullableString(record.ReviewDate),
		Score:             nullableFloat64(record.Score),
		Quote:             nullableString(record.Quote),
		PublicationName:   nullableString(record.PublicationName),
		PublicationSlug:   nullableString(record.PublicationSlug),
		AuthorName:        nullableString(record.AuthorName),
		AuthorSlug:        nullableString(record.AuthorSlug),
		SeasonLabel:       nullableString(record.SeasonLabel),
		Username:          nullableString(record.Username),
		UserSlug:          nullableString(record.UserSlug),
		ThumbsUp:          nullableInt64(record.ThumbsUp),
		ThumbsDown:        nullableInt64(record.ThumbsDown),
		VersionLabel:      nullableString(record.VersionLabel),
		SpoilerFlag:       nullableBoolInt(record.SpoilerFlag),
		SourcePayloadJson: payload,
		SourceCrawlRunID:  record.CrawlRunID,
		LastCrawledAt:     record.CrawledAt.UTC().Format(time.RFC3339),
	})
}

func insertReviewSnapshot(ctx context.Context, queries *sqlcgen.Queries, record domain.ReviewRecord) error {
	payload := normalizeSourcePayload(record.SourcePayloadJSON)
	return queries.InsertReviewSnapshot(ctx, sqlcgen.InsertReviewSnapshotParams{
		ReviewKey:         record.ReviewKey,
		CrawlRunID:        record.CrawlRunID,
		ExternalReviewID:  nullableString(record.ExternalReviewID),
		WorkHref:          record.WorkHref,
		Category:          string(record.Category),
		ReviewType:        string(record.ReviewType),
		PlatformKey:       strings.TrimSpace(record.PlatformKey),
		ReviewUrl:         nullableString(record.ReviewURL),
		ReviewDate:        nullableString(record.ReviewDate),
		Score:             nullableFloat64(record.Score),
		Quote:             nullableString(record.Quote),
		PublicationName:   nullableString(record.PublicationName),
		PublicationSlug:   nullableString(record.PublicationSlug),
		AuthorName:        nullableString(record.AuthorName),
		AuthorSlug:        nullableString(record.AuthorSlug),
		SeasonLabel:       nullableString(record.SeasonLabel),
		Username:          nullableString(record.Username),
		UserSlug:          nullableString(record.UserSlug),
		ThumbsUp:          nullableInt64(record.ThumbsUp),
		ThumbsDown:        nullableInt64(record.ThumbsDown),
		VersionLabel:      nullableString(record.VersionLabel),
		SpoilerFlag:       nullableBoolInt(record.SpoilerFlag),
		SourcePayloadJson: payload,
		CrawledAt:         record.CrawledAt.UTC().Format(time.RFC3339),
	})
}

func upsertReviewFetchStateRunning(ctx context.Context, queries *sqlcgen.Queries, scope domain.ReviewScope, attemptedAt time.Time, runID string) error {
	now := time.Now().UTC()
	return queries.UpsertReviewFetchStateRunning(ctx, sqlcgen.UpsertReviewFetchStateRunningParams{
		WorkHref:        scope.WorkHref,
		ReviewType:      string(scope.ReviewType),
		PlatformKey:     strings.TrimSpace(scope.PlatformKey),
		LastAttemptedAt: nullableTime(attemptedAt),
		LastRunID:       nullableString(runID),
		UpdatedAt:       now.Format(time.RFC3339),
	})
}

func upsertReviewFetchStateSucceeded(ctx context.Context, queries *sqlcgen.Queries, scope domain.ReviewScope, attemptedAt time.Time, fetchedAt time.Time, runID string) error {
	now := time.Now().UTC()
	return queries.UpsertReviewFetchStateSucceeded(ctx, sqlcgen.UpsertReviewFetchStateSucceededParams{
		WorkHref:        scope.WorkHref,
		ReviewType:      string(scope.ReviewType),
		PlatformKey:     strings.TrimSpace(scope.PlatformKey),
		LastAttemptedAt: nullableTime(attemptedAt),
		LastFetchedAt:   nullableTime(fetchedAt),
		LastRunID:       nullableString(runID),
		UpdatedAt:       now.Format(time.RFC3339),
	})
}

func upsertReviewFetchStateFailed(ctx context.Context, queries *sqlcgen.Queries, scope domain.ReviewScope, attemptedAt time.Time, runID string, failure ReviewFetchFailure) error {
	if strings.TrimSpace(failure.Message) == "" {
		failure.Message = "review processing failed"
	}
	now := time.Now().UTC()
	return queries.UpsertReviewFetchStateFailed(ctx, sqlcgen.UpsertReviewFetchStateFailedParams{
		WorkHref:        scope.WorkHref,
		ReviewType:      string(scope.ReviewType),
		PlatformKey:     strings.TrimSpace(scope.PlatformKey),
		LastAttemptedAt: nullableTime(attemptedAt),
		LastRunID:       nullableString(runID),
		LastError:       nullableString(failure.Message),
		LastErrorType:   nullableString(failure.ErrorType),
		LastErrorStage:  nullableString(failure.ErrorStage),
		UpdatedAt:       now.Format(time.RFC3339),
	})
}

func normalizeSourcePayload(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "{}"
	}

	var js json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &js); err != nil {
		return "{}"
	}
	normalized, err := json.Marshal(js)
	if err != nil {
		return "{}"
	}
	return string(normalized)
}

func nullableFloat64(value *float64) sql.NullFloat64 {
	if value == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: *value, Valid: true}
}

func nullableInt64(value *int64) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *value, Valid: true}
}

func nullableBoolInt(value *bool) sql.NullInt64 {
	if value == nil {
		return sql.NullInt64{}
	}
	if *value {
		return sql.NullInt64{Int64: 1, Valid: true}
	}
	return sql.NullInt64{Int64: 0, Valid: true}
}

func IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
