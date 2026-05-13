package cli

import (
	"database/sql"
	"sort"
	"strconv"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	sqlcgen "github.com/gofurry/metacritic-harvester/internal/storage/sqlcgen"
)

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

type reviewCompareView struct {
	ReviewKey        string `json:"review_key"`
	WorkHref         string `json:"work_href"`
	Category         string `json:"category"`
	ReviewType       string `json:"review_type"`
	PlatformKey      string `json:"platform_key"`
	FromScore        string `json:"from_score,omitempty"`
	ToScore          string `json:"to_score,omitempty"`
	ScoreDiff        string `json:"score_diff,omitempty"`
	FromQuote        string `json:"from_quote,omitempty"`
	ToQuote          string `json:"to_quote,omitempty"`
	FromThumbsUp     string `json:"from_thumbs_up,omitempty"`
	ToThumbsUp       string `json:"to_thumbs_up,omitempty"`
	FromThumbsDown   string `json:"from_thumbs_down,omitempty"`
	ToThumbsDown     string `json:"to_thumbs_down,omitempty"`
	FromVersionLabel string `json:"from_version_label,omitempty"`
	ToVersionLabel   string `json:"to_version_label,omitempty"`
	FromSpoilerFlag  string `json:"from_spoiler_flag,omitempty"`
	ToSpoilerFlag    string `json:"to_spoiler_flag,omitempty"`
	ChangeType       string `json:"change_type"`
}

type reviewExportRecord struct {
	ReviewKey         string
	ExternalReviewID  string
	WorkHref          string
	Category          string
	ReviewType        string
	PlatformKey       string
	ReviewURL         string
	ReviewDate        string
	Score             string
	Quote             string
	PublicationName   string
	PublicationSlug   string
	AuthorName        string
	AuthorSlug        string
	SeasonLabel       string
	Username          string
	UserSlug          string
	ThumbsUp          string
	ThumbsDown        string
	VersionLabel      string
	SpoilerFlag       string
	SourcePayloadJSON string
	RunID             string
	LastCrawledAt     string
}

type reviewFlatView struct {
	RunID            string `json:"run_id"`
	ReviewKey        string `json:"review_key"`
	ExternalReviewID string `json:"external_review_id,omitempty"`
	WorkHref         string `json:"work_href"`
	Category         string `json:"category"`
	ReviewType       string `json:"review_type"`
	PlatformKey      string `json:"platform_key,omitempty"`
	ReviewURL        string `json:"review_url,omitempty"`
	ReviewDate       string `json:"review_date,omitempty"`
	Score            string `json:"score,omitempty"`
	Quote            string `json:"quote,omitempty"`
	PublicationName  string `json:"publication_name,omitempty"`
	PublicationSlug  string `json:"publication_slug,omitempty"`
	AuthorName       string `json:"author_name,omitempty"`
	AuthorSlug       string `json:"author_slug,omitempty"`
	SeasonLabel      string `json:"season_label,omitempty"`
	Username         string `json:"username,omitempty"`
	UserSlug         string `json:"user_slug,omitempty"`
	ThumbsUp         string `json:"thumbs_up,omitempty"`
	ThumbsDown       string `json:"thumbs_down,omitempty"`
	VersionLabel     string `json:"version_label,omitempty"`
	SpoilerFlag      string `json:"spoiler_flag,omitempty"`
	LastCrawledAt    string `json:"last_crawled_at"`
}

type reviewSummaryView struct {
	RunID                string  `json:"run_id"`
	Category             string  `json:"category"`
	ReviewType           string  `json:"review_type"`
	PlatformKey          string  `json:"platform_key,omitempty"`
	RowCount             int     `json:"row_count"`
	ScoredCount          int     `json:"scored_count"`
	AvgScore             float64 `json:"avg_score,omitempty"`
	WithQuoteCount       int     `json:"with_quote_count"`
	WithPublicationCount int     `json:"with_publication_count"`
	WithUsernameCount    int     `json:"with_username_count"`
}

func mapLatestReviews(rows []sqlcgen.LatestReview) []reviewView {
	result := make([]reviewView, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewView{
			ReviewKey:         row.ReviewKey,
			ExternalReviewID:  nullStringValue(row.ExternalReviewID),
			WorkHref:          row.WorkHref,
			Category:          row.Category,
			ReviewType:        row.ReviewType,
			PlatformKey:       row.PlatformKey,
			ReviewURL:         nullStringValue(row.ReviewUrl),
			ReviewDate:        nullStringValue(row.ReviewDate),
			Score:             nullFloat64Value(row.Score),
			Quote:             nullStringValue(row.Quote),
			PublicationName:   nullStringValue(row.PublicationName),
			PublicationSlug:   nullStringValue(row.PublicationSlug),
			AuthorName:        nullStringValue(row.AuthorName),
			AuthorSlug:        nullStringValue(row.AuthorSlug),
			SeasonLabel:       nullStringValue(row.SeasonLabel),
			Username:          nullStringValue(row.Username),
			UserSlug:          nullStringValue(row.UserSlug),
			ThumbsUp:          nullInt64Value(row.ThumbsUp),
			ThumbsDown:        nullInt64Value(row.ThumbsDown),
			VersionLabel:      nullStringValue(row.VersionLabel),
			SpoilerFlag:       nullBoolIntValue(row.SpoilerFlag),
			SourcePayloadJSON: row.SourcePayloadJson,
			SourceCrawlRunID:  row.SourceCrawlRunID,
			LastCrawledAt:     row.LastCrawledAt,
		})
	}
	return result
}

func mapLatestReviewsForExport(rows []sqlcgen.LatestReview) []reviewExportRecord {
	result := make([]reviewExportRecord, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewExportRecord{
			ReviewKey:         row.ReviewKey,
			ExternalReviewID:  nullStringValue(row.ExternalReviewID),
			WorkHref:          row.WorkHref,
			Category:          row.Category,
			ReviewType:        row.ReviewType,
			PlatformKey:       row.PlatformKey,
			ReviewURL:         nullStringValue(row.ReviewUrl),
			ReviewDate:        nullStringValue(row.ReviewDate),
			Score:             nullFloat64Value(row.Score),
			Quote:             nullStringValue(row.Quote),
			PublicationName:   nullStringValue(row.PublicationName),
			PublicationSlug:   nullStringValue(row.PublicationSlug),
			AuthorName:        nullStringValue(row.AuthorName),
			AuthorSlug:        nullStringValue(row.AuthorSlug),
			SeasonLabel:       nullStringValue(row.SeasonLabel),
			Username:          nullStringValue(row.Username),
			UserSlug:          nullStringValue(row.UserSlug),
			ThumbsUp:          nullInt64Value(row.ThumbsUp),
			ThumbsDown:        nullInt64Value(row.ThumbsDown),
			VersionLabel:      nullStringValue(row.VersionLabel),
			SpoilerFlag:       nullBoolIntValue(row.SpoilerFlag),
			SourcePayloadJSON: row.SourcePayloadJson,
			RunID:             row.SourceCrawlRunID,
			LastCrawledAt:     row.LastCrawledAt,
		})
	}
	return result
}

func mapReviewSnapshotsForExport(rows []sqlcgen.ReviewSnapshot) []reviewExportRecord {
	result := make([]reviewExportRecord, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewExportRecord{
			ReviewKey:         row.ReviewKey,
			ExternalReviewID:  nullStringValue(row.ExternalReviewID),
			WorkHref:          row.WorkHref,
			Category:          row.Category,
			ReviewType:        row.ReviewType,
			PlatformKey:       row.PlatformKey,
			ReviewURL:         nullStringValue(row.ReviewUrl),
			ReviewDate:        nullStringValue(row.ReviewDate),
			Score:             nullFloat64Value(row.Score),
			Quote:             nullStringValue(row.Quote),
			PublicationName:   nullStringValue(row.PublicationName),
			PublicationSlug:   nullStringValue(row.PublicationSlug),
			AuthorName:        nullStringValue(row.AuthorName),
			AuthorSlug:        nullStringValue(row.AuthorSlug),
			SeasonLabel:       nullStringValue(row.SeasonLabel),
			Username:          nullStringValue(row.Username),
			UserSlug:          nullStringValue(row.UserSlug),
			ThumbsUp:          nullInt64Value(row.ThumbsUp),
			ThumbsDown:        nullInt64Value(row.ThumbsDown),
			VersionLabel:      nullStringValue(row.VersionLabel),
			SpoilerFlag:       nullBoolIntValue(row.SpoilerFlag),
			SourcePayloadJSON: row.SourcePayloadJson,
			RunID:             row.CrawlRunID,
			LastCrawledAt:     row.CrawledAt,
		})
	}
	return result
}

func mapReviewExportRecordsToRaw(rows []reviewExportRecord) []reviewView {
	result := make([]reviewView, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewView{
			ReviewKey:         row.ReviewKey,
			ExternalReviewID:  row.ExternalReviewID,
			WorkHref:          row.WorkHref,
			Category:          row.Category,
			ReviewType:        row.ReviewType,
			PlatformKey:       row.PlatformKey,
			ReviewURL:         row.ReviewURL,
			ReviewDate:        row.ReviewDate,
			Score:             row.Score,
			Quote:             row.Quote,
			PublicationName:   row.PublicationName,
			PublicationSlug:   row.PublicationSlug,
			AuthorName:        row.AuthorName,
			AuthorSlug:        row.AuthorSlug,
			SeasonLabel:       row.SeasonLabel,
			Username:          row.Username,
			UserSlug:          row.UserSlug,
			ThumbsUp:          row.ThumbsUp,
			ThumbsDown:        row.ThumbsDown,
			VersionLabel:      row.VersionLabel,
			SpoilerFlag:       row.SpoilerFlag,
			SourcePayloadJSON: row.SourcePayloadJSON,
			SourceCrawlRunID:  row.RunID,
			LastCrawledAt:     row.LastCrawledAt,
		})
	}
	return result
}

func mapReviewExportRecordsToFlat(rows []reviewExportRecord) []reviewFlatView {
	result := make([]reviewFlatView, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewFlatView{
			RunID:            row.RunID,
			ReviewKey:        row.ReviewKey,
			ExternalReviewID: row.ExternalReviewID,
			WorkHref:         row.WorkHref,
			Category:         row.Category,
			ReviewType:       row.ReviewType,
			PlatformKey:      row.PlatformKey,
			ReviewURL:        row.ReviewURL,
			ReviewDate:       row.ReviewDate,
			Score:            row.Score,
			Quote:            row.Quote,
			PublicationName:  row.PublicationName,
			PublicationSlug:  row.PublicationSlug,
			AuthorName:       row.AuthorName,
			AuthorSlug:       row.AuthorSlug,
			SeasonLabel:      row.SeasonLabel,
			Username:         row.Username,
			UserSlug:         row.UserSlug,
			ThumbsUp:         row.ThumbsUp,
			ThumbsDown:       row.ThumbsDown,
			VersionLabel:     row.VersionLabel,
			SpoilerFlag:      row.SpoilerFlag,
			LastCrawledAt:    row.LastCrawledAt,
		})
	}
	return result
}

func summarizeReviewExportRecords(rows []reviewExportRecord) []reviewSummaryView {
	type groupKey struct {
		runID       string
		category    string
		reviewType  string
		platformKey string
	}
	type aggregate struct {
		view       reviewSummaryView
		totalScore float64
	}

	groups := make(map[groupKey]*aggregate, len(rows))
	for _, row := range rows {
		key := groupKey{
			runID:       row.RunID,
			category:    row.Category,
			reviewType:  row.ReviewType,
			platformKey: row.PlatformKey,
		}
		group, ok := groups[key]
		if !ok {
			group = &aggregate{
				view: reviewSummaryView{
					RunID:       row.RunID,
					Category:    row.Category,
					ReviewType:  row.ReviewType,
					PlatformKey: row.PlatformKey,
				},
			}
			groups[key] = group
		}
		group.view.RowCount++
		if row.Score != "" {
			group.view.ScoredCount++
			if parsed, err := parseStringFloat64(row.Score); err == nil {
				group.totalScore += parsed
			}
		}
		if row.Quote != "" {
			group.view.WithQuoteCount++
		}
		if row.PublicationName != "" {
			group.view.WithPublicationCount++
		}
		if row.Username != "" {
			group.view.WithUsernameCount++
		}
	}

	result := make([]reviewSummaryView, 0, len(groups))
	for _, group := range groups {
		if group.view.ScoredCount > 0 {
			group.view.AvgScore = group.totalScore / float64(group.view.ScoredCount)
		}
		result = append(result, group.view)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].RunID != result[j].RunID {
			return result[i].RunID < result[j].RunID
		}
		if result[i].Category != result[j].Category {
			return result[i].Category < result[j].Category
		}
		if result[i].ReviewType != result[j].ReviewType {
			return result[i].ReviewType < result[j].ReviewType
		}
		return result[i].PlatformKey < result[j].PlatformKey
	})
	return result
}

func mapReviewCompareRows(rows []sqlcgen.CompareReviewSnapshotsRow) []reviewCompareView {
	result := make([]reviewCompareView, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewCompareView{
			ReviewKey:        row.ReviewKey,
			WorkHref:         row.WorkHref,
			Category:         row.Category,
			ReviewType:       row.ReviewType,
			PlatformKey:      row.PlatformKey,
			FromScore:        nullFloat64Value(row.FromScore),
			ToScore:          nullFloat64Value(row.ToScore),
			ScoreDiff:        interfaceValueString(row.ScoreDiff),
			FromQuote:        nullStringValue(row.FromQuote),
			ToQuote:          nullStringValue(row.ToQuote),
			FromThumbsUp:     nullInt64Value(row.FromThumbsUp),
			ToThumbsUp:       nullInt64Value(row.ToThumbsUp),
			FromThumbsDown:   nullInt64Value(row.FromThumbsDown),
			ToThumbsDown:     nullInt64Value(row.ToThumbsDown),
			FromVersionLabel: nullStringValue(row.FromVersionLabel),
			ToVersionLabel:   nullStringValue(row.ToVersionLabel),
			FromSpoilerFlag:  nullBoolIntValue(row.FromSpoilerFlag),
			ToSpoilerFlag:    nullBoolIntValue(row.ToSpoilerFlag),
			ChangeType:       row.ChangeType,
		})
	}
	return result
}

func reviewScoreString(value *float64) string {
	if value == nil {
		return ""
	}
	return nullFloat64Value(sql.NullFloat64{Float64: *value, Valid: true})
}

func reviewInt64String(value *int64) string {
	if value == nil {
		return ""
	}
	return nullInt64Value(sql.NullInt64{Int64: *value, Valid: true})
}

func reviewBoolString(value *bool) string {
	if value == nil {
		return ""
	}
	return nullBoolIntValue(sql.NullInt64{Int64: func() int64 {
		if *value {
			return 1
		}
		return 0
	}(), Valid: true})
}

func reviewReviewTypeValue(value domain.ReviewType) string {
	return string(value)
}

func parseStringFloat64(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}
