package serve

import (
	"database/sql"
	"encoding/json"
	"strconv"

	sqlcgen "github.com/gofurry/metacritic-harvester/internal/storage/sqlcgen"
)

type healthView struct {
	OK bool `json:"ok"`
}

type configView struct {
	Addr        string `json:"addr"`
	DBPath      string `json:"db_path"`
	FullStack   bool   `json:"full_stack"`
	EnableWrite bool   `json:"enable_write"`
}

type crawlRunView struct {
	RunID      string `json:"run_id"`
	Source     string `json:"source"`
	TaskName   string `json:"task_name"`
	Category   string `json:"category"`
	Metric     string `json:"metric"`
	FilterKey  string `json:"filter_key"`
	Status     string `json:"status"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
	Error      string `json:"error,omitempty"`
}

type latestEntryView struct {
	WorkHref         string `json:"work_href"`
	Category         string `json:"category"`
	Metric           string `json:"metric"`
	FilterKey        string `json:"filter_key"`
	PageNo           int64  `json:"page_no"`
	RankNo           int64  `json:"rank_no"`
	Metascore        string `json:"metascore,omitempty"`
	UserScore        string `json:"user_score,omitempty"`
	LastCrawledAt    string `json:"last_crawled_at"`
	SourceCrawlRunID string `json:"source_crawl_run_id"`
}

type detailView struct {
	WorkHref             string         `json:"work_href"`
	Category             string         `json:"category"`
	Title                string         `json:"title"`
	Summary              string         `json:"summary,omitempty"`
	ReleaseDate          string         `json:"release_date,omitempty"`
	Metascore            string         `json:"metascore,omitempty"`
	MetascoreSentiment   string         `json:"metascore_sentiment,omitempty"`
	MetascoreReviewCount int            `json:"metascore_review_count,omitempty"`
	UserScore            string         `json:"user_score,omitempty"`
	UserScoreSentiment   string         `json:"user_score_sentiment,omitempty"`
	UserScoreCount       int            `json:"user_score_count,omitempty"`
	Rating               string         `json:"rating,omitempty"`
	Duration             string         `json:"duration,omitempty"`
	Tagline              string         `json:"tagline,omitempty"`
	LastFetchedAt        string         `json:"last_fetched_at"`
	Details              map[string]any `json:"details"`
}

type reviewView struct {
	ReviewKey         string `json:"review_key"`
	ExternalReviewID  string `json:"external_review_id,omitempty"`
	WorkHref          string `json:"work_href"`
	Category          string `json:"category"`
	ReviewType        string `json:"review_type"`
	PlatformKey       string `json:"platform_key,omitempty"`
	ReviewURL         string `json:"review_url,omitempty"`
	ReviewDate        string `json:"review_date,omitempty"`
	Score             string `json:"score,omitempty"`
	Quote             string `json:"quote,omitempty"`
	PublicationName   string `json:"publication_name,omitempty"`
	PublicationSlug   string `json:"publication_slug,omitempty"`
	AuthorName        string `json:"author_name,omitempty"`
	AuthorSlug        string `json:"author_slug,omitempty"`
	SeasonLabel       string `json:"season_label,omitempty"`
	Username          string `json:"username,omitempty"`
	UserSlug          string `json:"user_slug,omitempty"`
	ThumbsUp          string `json:"thumbs_up,omitempty"`
	ThumbsDown        string `json:"thumbs_down,omitempty"`
	VersionLabel      string `json:"version_label,omitempty"`
	SpoilerFlag       string `json:"spoiler_flag,omitempty"`
	SourcePayloadJSON string `json:"source_payload_json,omitempty"`
	SourceCrawlRunID  string `json:"source_crawl_run_id"`
	LastCrawledAt     string `json:"last_crawled_at"`
}

type detailFetchStateView struct {
	WorkHref        string `json:"work_href"`
	Status          string `json:"status"`
	LastAttemptedAt string `json:"last_attempted_at,omitempty"`
	LastFetchedAt   string `json:"last_fetched_at,omitempty"`
	LastRunID       string `json:"last_run_id,omitempty"`
	LastError       string `json:"last_error,omitempty"`
	LastErrorType   string `json:"last_error_type,omitempty"`
	LastErrorStage  string `json:"last_error_stage,omitempty"`
	UpdatedAt       string `json:"updated_at,omitempty"`
}

type reviewFetchStateView struct {
	WorkHref        string `json:"work_href"`
	ReviewType      string `json:"review_type"`
	PlatformKey     string `json:"platform_key,omitempty"`
	Status          string `json:"status"`
	LastAttemptedAt string `json:"last_attempted_at,omitempty"`
	LastFetchedAt   string `json:"last_fetched_at,omitempty"`
	LastRunID       string `json:"last_run_id,omitempty"`
	LastError       string `json:"last_error,omitempty"`
	LastErrorType   string `json:"last_error_type,omitempty"`
	LastErrorStage  string `json:"last_error_stage,omitempty"`
	UpdatedAt       string `json:"updated_at,omitempty"`
}

func mapCrawlRuns(rows []sqlcgen.CrawlRun) []crawlRunView {
	result := make([]crawlRunView, 0, len(rows))
	for _, row := range rows {
		result = append(result, crawlRunView{
			RunID:      row.RunID,
			Source:     row.Source,
			TaskName:   row.TaskName,
			Category:   row.Category,
			Metric:     row.Metric,
			FilterKey:  row.FilterKey,
			Status:     row.Status,
			StartedAt:  row.StartedAt,
			FinishedAt: nullString(row.FinishedAt),
			Error:      nullString(row.ErrorMessage),
		})
	}
	return result
}

func mapLatestEntries(rows []sqlcgen.LatestListEntry) []latestEntryView {
	result := make([]latestEntryView, 0, len(rows))
	for _, row := range rows {
		result = append(result, latestEntryView{
			WorkHref:         row.WorkHref,
			Category:         row.Category,
			Metric:           row.Metric,
			FilterKey:        row.FilterKey,
			PageNo:           row.PageNo,
			RankNo:           row.RankNo,
			Metascore:        nullString(row.Metascore),
			UserScore:        nullString(row.UserScore),
			LastCrawledAt:    row.LastCrawledAt,
			SourceCrawlRunID: row.SourceCrawlRunID,
		})
	}
	return result
}

func mapWorkDetails(rows []sqlcgen.WorkDetail) ([]detailView, error) {
	result := make([]detailView, 0, len(rows))
	for _, row := range rows {
		details := map[string]any{}
		if raw := row.DetailsJson; raw != "" {
			if err := json.Unmarshal([]byte(raw), &details); err != nil {
				return nil, err
			}
		}
		result = append(result, detailView{
			WorkHref:             row.WorkHref,
			Category:             row.Category,
			Title:                row.Title,
			Summary:              nullString(row.Summary),
			ReleaseDate:          nullString(row.ReleaseDate),
			Metascore:            nullString(row.Metascore),
			MetascoreSentiment:   nullString(row.MetascoreSentiment),
			MetascoreReviewCount: nullInt(row.MetascoreReviewCount),
			UserScore:            nullString(row.UserScore),
			UserScoreSentiment:   nullString(row.UserScoreSentiment),
			UserScoreCount:       nullInt(row.UserScoreCount),
			Rating:               nullString(row.Rating),
			Duration:             nullString(row.Duration),
			Tagline:              nullString(row.Tagline),
			LastFetchedAt:        row.LastFetchedAt,
			Details:              details,
		})
	}
	return result, nil
}

func mapLatestReviews(rows []sqlcgen.LatestReview) []reviewView {
	result := make([]reviewView, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewView{
			ReviewKey:         row.ReviewKey,
			ExternalReviewID:  nullString(row.ExternalReviewID),
			WorkHref:          row.WorkHref,
			Category:          row.Category,
			ReviewType:        row.ReviewType,
			PlatformKey:       row.PlatformKey,
			ReviewURL:         nullString(row.ReviewUrl),
			ReviewDate:        nullString(row.ReviewDate),
			Score:             nullFloat(row.Score),
			Quote:             nullString(row.Quote),
			PublicationName:   nullString(row.PublicationName),
			PublicationSlug:   nullString(row.PublicationSlug),
			AuthorName:        nullString(row.AuthorName),
			AuthorSlug:        nullString(row.AuthorSlug),
			SeasonLabel:       nullString(row.SeasonLabel),
			Username:          nullString(row.Username),
			UserSlug:          nullString(row.UserSlug),
			ThumbsUp:          nullInt64String(row.ThumbsUp),
			ThumbsDown:        nullInt64String(row.ThumbsDown),
			VersionLabel:      nullString(row.VersionLabel),
			SpoilerFlag:       nullBoolString(row.SpoilerFlag),
			SourcePayloadJSON: row.SourcePayloadJson,
			SourceCrawlRunID:  row.SourceCrawlRunID,
			LastCrawledAt:     row.LastCrawledAt,
		})
	}
	return result
}

func mapDetailFetchState(row sqlcgen.DetailFetchState) detailFetchStateView {
	return detailFetchStateView{
		WorkHref:        row.WorkHref,
		Status:          row.Status,
		LastAttemptedAt: nullString(row.LastAttemptedAt),
		LastFetchedAt:   nullString(row.LastFetchedAt),
		LastRunID:       nullString(row.LastRunID),
		LastError:       nullString(row.LastError),
		LastErrorType:   nullString(row.LastErrorType),
		LastErrorStage:  nullString(row.LastErrorStage),
		UpdatedAt:       row.UpdatedAt,
	}
}

func mapReviewFetchState(row sqlcgen.ReviewFetchState) reviewFetchStateView {
	return reviewFetchStateView{
		WorkHref:        row.WorkHref,
		ReviewType:      row.ReviewType,
		PlatformKey:     row.PlatformKey,
		Status:          row.Status,
		LastAttemptedAt: nullString(row.LastAttemptedAt),
		LastFetchedAt:   nullString(row.LastFetchedAt),
		LastRunID:       nullString(row.LastRunID),
		LastError:       nullString(row.LastError),
		LastErrorType:   nullString(row.LastErrorType),
		LastErrorStage:  nullString(row.LastErrorStage),
		UpdatedAt:       row.UpdatedAt,
	}
}

func nullString(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}

func nullInt(v sql.NullInt64) int {
	if !v.Valid {
		return 0
	}
	return int(v.Int64)
}

func nullInt64String(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	return strconv.FormatInt(v.Int64, 10)
}

func nullFloat(v sql.NullFloat64) string {
	if !v.Valid {
		return ""
	}
	return strconv.FormatFloat(v.Float64, 'f', -1, 64)
}

func nullBoolString(v sql.NullInt64) string {
	if !v.Valid {
		return ""
	}
	if v.Int64 != 0 {
		return "true"
	}
	return "false"
}
