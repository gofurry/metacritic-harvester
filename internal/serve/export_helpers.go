package serve

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/GoFurry/metacritic-harvester/internal/domain"
	"github.com/GoFurry/metacritic-harvester/internal/storage"
	sqlcgen "github.com/GoFurry/metacritic-harvester/internal/storage/sqlcgen"
)

const (
	exportFormatCSV      = "csv"
	exportFormatJSON     = "json"
	exportProfileRaw     = "raw"
	exportProfileFlat    = "flat"
	exportProfileSummary = "summary"
)

type exportDownload struct {
	ContentType string
	Filename    string
	Body        []byte
}

type latestSummaryView struct {
	RunID             string `json:"run_id"`
	Category          string `json:"category"`
	Metric            string `json:"metric"`
	FilterKey         string `json:"filter_key"`
	RowCount          int    `json:"row_count"`
	DistinctWorkCount int    `json:"distinct_work_count"`
	MinRank           int64  `json:"min_rank"`
	MaxRank           int64  `json:"max_rank"`
}

type detailExportRecord struct {
	WorkHref             string
	RunID                string
	Category             string
	Title                string
	Summary              string
	ReleaseDate          string
	Metascore            string
	MetascoreSentiment   string
	MetascoreReviewCount int
	UserScore            string
	UserScoreSentiment   string
	UserScoreCount       int
	Rating               string
	Duration             string
	Tagline              string
	LastFetchedAt        string
	Details              domain.WorkDetailExtras
	RawDetailsJSON       string
}

type detailFlatView struct {
	RunID                  string `json:"run_id,omitempty"`
	WorkHref               string `json:"work_href"`
	Category               string `json:"category"`
	Title                  string `json:"title"`
	ReleaseDate            string `json:"release_date,omitempty"`
	Metascore              string `json:"metascore,omitempty"`
	UserScore              string `json:"user_score,omitempty"`
	Rating                 string `json:"rating,omitempty"`
	Duration               string `json:"duration,omitempty"`
	Tagline                string `json:"tagline,omitempty"`
	LastFetchedAt          string `json:"last_fetched_at"`
	GenresCSV              string `json:"genres_csv,omitempty"`
	PlatformsCSV           string `json:"platforms_csv,omitempty"`
	DevelopersCSV          string `json:"developers_csv,omitempty"`
	PublishersCSV          string `json:"publishers_csv,omitempty"`
	ProductionCompaniesCSV string `json:"production_companies_csv,omitempty"`
	DirectorsCSV           string `json:"directors_csv,omitempty"`
	WritersCSV             string `json:"writers_csv,omitempty"`
	AwardsCount            int    `json:"awards_count"`
	SeasonsCount           int    `json:"seasons_count"`
}

type detailSummaryView struct {
	RunID              string `json:"run_id,omitempty"`
	Category           string `json:"category"`
	RowCount           int    `json:"row_count"`
	WithMetascoreCount int    `json:"with_metascore_count"`
	WithUserScoreCount int    `json:"with_user_score_count"`
	WithRatingCount    int    `json:"with_rating_count"`
	WithDurationCount  int    `json:"with_duration_count"`
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

func isValidExportFormat(raw string) bool {
	return raw == exportFormatCSV || raw == exportFormatJSON
}

func isValidExportProfile(raw string) bool {
	switch raw {
	case exportProfileRaw, exportProfileFlat, exportProfileSummary:
		return true
	default:
		return false
	}
}

func buildLatestExport(ctx context.Context, repo *storage.Repository, filter storage.ListLatestEntriesFilter, runID string, format string, profile string) (exportDownload, error) {
	var rows []latestEntryView
	if runID == "" {
		entries, err := repo.ListLatestEntries(ctx, filter)
		if err != nil {
			return exportDownload{}, err
		}
		rows = mapLatestEntries(entries)
	} else {
		entries, err := repo.ListListEntriesByRun(ctx, runID, filter)
		if err != nil {
			return exportDownload{}, err
		}
		rows = mapSnapshotListEntries(entries)
	}

	filename := buildExportFilename("latest", format, profile, runID)
	if profile == exportProfileSummary {
		summary := summarizeLatestEntries(rows)
		return encodeDownload(filename, format, summary, func() ([][]string, []string) {
			csvRows := make([][]string, 0, len(summary))
			for _, row := range summary {
				csvRows = append(csvRows, []string{
					row.RunID, row.Category, row.Metric, row.FilterKey,
					strconv.Itoa(row.RowCount), strconv.Itoa(row.DistinctWorkCount),
					strconv.FormatInt(row.MinRank, 10), strconv.FormatInt(row.MaxRank, 10),
				})
			}
			return csvRows, []string{"run_id", "category", "metric", "filter_key", "row_count", "distinct_work_count", "min_rank", "max_rank"}
		})
	}

	return encodeDownload(filename, format, rows, func() ([][]string, []string) {
		csvRows := make([][]string, 0, len(rows))
		for _, row := range rows {
			csvRows = append(csvRows, []string{
				row.WorkHref, row.Category, row.Metric, row.FilterKey,
				strconv.FormatInt(row.PageNo, 10), strconv.FormatInt(row.RankNo, 10),
				row.Metascore, row.UserScore, row.LastCrawledAt, row.SourceCrawlRunID,
			})
		}
		return csvRows, []string{"work_href", "category", "metric", "filter_key", "page_no", "rank_no", "metascore", "user_score", "last_crawled_at", "source_crawl_run_id"}
	})
}

func buildDetailExport(ctx context.Context, repo *storage.Repository, filter storage.ListWorkDetailsFilter, runID string, format string, profile string) (exportDownload, error) {
	var records []detailExportRecord
	var err error
	if runID == "" {
		rows, runErr := repo.ListWorkDetailsForExport(ctx, filter)
		if runErr != nil {
			return exportDownload{}, runErr
		}
		records, err = mapWorkDetailsForExport(rows)
	} else {
		rows, runErr := repo.ListWorkDetailSnapshotsByRun(ctx, runID, filter)
		if runErr != nil {
			return exportDownload{}, runErr
		}
		records, err = mapWorkDetailSnapshots(rows)
	}
	if err != nil {
		return exportDownload{}, err
	}

	filename := buildExportFilename("detail", format, profile, runID)
	switch profile {
	case exportProfileSummary:
		summary := summarizeDetailExportRecords(records)
		return encodeDownload(filename, format, summary, func() ([][]string, []string) {
			csvRows := make([][]string, 0, len(summary))
			for _, row := range summary {
				csvRows = append(csvRows, []string{
					row.RunID, row.Category, strconv.Itoa(row.RowCount),
					strconv.Itoa(row.WithMetascoreCount), strconv.Itoa(row.WithUserScoreCount),
					strconv.Itoa(row.WithRatingCount), strconv.Itoa(row.WithDurationCount),
				})
			}
			return csvRows, []string{"run_id", "category", "row_count", "with_metascore_count", "with_user_score_count", "with_rating_count", "with_duration_count"}
		})
	case exportProfileFlat:
		flat := mapDetailExportRecordsToFlat(records)
		return encodeDownload(filename, format, flat, func() ([][]string, []string) {
			csvRows := make([][]string, 0, len(flat))
			for _, row := range flat {
				csvRows = append(csvRows, []string{
					row.RunID, row.WorkHref, row.Category, row.Title, row.ReleaseDate, row.Metascore,
					row.UserScore, row.Rating, row.Duration, row.Tagline, row.LastFetchedAt,
					row.GenresCSV, row.PlatformsCSV, row.DevelopersCSV, row.PublishersCSV,
					row.ProductionCompaniesCSV, row.DirectorsCSV, row.WritersCSV,
					strconv.Itoa(row.AwardsCount), strconv.Itoa(row.SeasonsCount),
				})
			}
			return csvRows, []string{"run_id", "work_href", "category", "title", "release_date", "metascore", "user_score", "rating", "duration", "tagline", "last_fetched_at", "genres_csv", "platforms_csv", "developers_csv", "publishers_csv", "production_companies_csv", "directors_csv", "writers_csv", "awards_count", "seasons_count"}
		})
	default:
		raw := mapDetailExportRecordsToRaw(records)
		return encodeDownload(filename, format, raw, func() ([][]string, []string) {
			csvRows := make([][]string, 0, len(records))
			for _, row := range records {
				csvRows = append(csvRows, []string{
					row.WorkHref, row.Category, row.Title, row.ReleaseDate, row.Metascore, row.UserScore,
					row.Rating, row.Duration, row.Tagline, row.LastFetchedAt, row.RawDetailsJSON,
				})
			}
			return csvRows, []string{"work_href", "category", "title", "release_date", "metascore", "user_score", "rating", "duration", "tagline", "last_fetched_at", "details_json"}
		})
	}
}

func buildReviewExport(ctx context.Context, repo *storage.Repository, filter storage.ListLatestReviewsFilter, runID string, format string, profile string) (exportDownload, error) {
	var records []reviewExportRecord
	if runID == "" {
		rows, err := repo.ListLatestReviews(ctx, filter)
		if err != nil {
			return exportDownload{}, err
		}
		records = mapLatestReviewsForExport(rows)
	} else {
		rows, err := repo.ListReviewSnapshotsByRun(ctx, runID, filter)
		if err != nil {
			return exportDownload{}, err
		}
		records = mapReviewSnapshotsForExport(rows)
	}

	filename := buildExportFilename("review", format, profile, runID)
	switch profile {
	case exportProfileSummary:
		summary := summarizeReviewExportRecords(records)
		return encodeDownload(filename, format, summary, func() ([][]string, []string) {
			csvRows := make([][]string, 0, len(summary))
			for _, row := range summary {
				csvRows = append(csvRows, []string{
					row.RunID, row.Category, row.ReviewType, row.PlatformKey,
					strconv.Itoa(row.RowCount), strconv.Itoa(row.ScoredCount),
					strconv.FormatFloat(row.AvgScore, 'f', -1, 64),
					strconv.Itoa(row.WithQuoteCount), strconv.Itoa(row.WithPublicationCount), strconv.Itoa(row.WithUsernameCount),
				})
			}
			return csvRows, []string{"run_id", "category", "review_type", "platform_key", "row_count", "scored_count", "avg_score", "with_quote_count", "with_publication_count", "with_username_count"}
		})
	case exportProfileFlat:
		flat := mapReviewExportRecordsToFlat(records)
		return encodeDownload(filename, format, flat, func() ([][]string, []string) {
			csvRows := make([][]string, 0, len(flat))
			for _, row := range flat {
				csvRows = append(csvRows, []string{
					row.RunID, row.ReviewKey, row.ExternalReviewID, row.WorkHref, row.Category, row.ReviewType,
					row.PlatformKey, row.ReviewURL, row.ReviewDate, row.Score, row.Quote, row.PublicationName,
					row.PublicationSlug, row.AuthorName, row.AuthorSlug, row.SeasonLabel, row.Username, row.UserSlug,
					row.ThumbsUp, row.ThumbsDown, row.VersionLabel, row.SpoilerFlag, row.LastCrawledAt,
				})
			}
			return csvRows, []string{"run_id", "review_key", "external_review_id", "work_href", "category", "review_type", "platform_key", "review_url", "review_date", "score", "quote", "publication_name", "publication_slug", "author_name", "author_slug", "season_label", "username", "user_slug", "thumbs_up", "thumbs_down", "version_label", "spoiler_flag", "last_crawled_at"}
		})
	default:
		raw := mapReviewExportRecordsToRaw(records)
		return encodeDownload(filename, format, raw, func() ([][]string, []string) {
			csvRows := make([][]string, 0, len(raw))
			for _, row := range raw {
				csvRows = append(csvRows, []string{
					row.ReviewKey, row.ExternalReviewID, row.WorkHref, row.Category, row.ReviewType, row.PlatformKey,
					row.ReviewURL, row.ReviewDate, row.Score, row.Quote, row.PublicationName, row.PublicationSlug,
					row.AuthorName, row.AuthorSlug, row.SeasonLabel, row.Username, row.UserSlug, row.ThumbsUp, row.ThumbsDown,
					row.VersionLabel, row.SpoilerFlag, row.SourcePayloadJSON, row.SourceCrawlRunID, row.LastCrawledAt,
				})
			}
			return csvRows, []string{"review_key", "external_review_id", "work_href", "category", "review_type", "platform_key", "review_url", "review_date", "score", "quote", "publication_name", "publication_slug", "author_name", "author_slug", "season_label", "username", "user_slug", "thumbs_up", "thumbs_down", "version_label", "spoiler_flag", "source_payload_json", "source_crawl_run_id", "last_crawled_at"}
		})
	}
}

func encodeDownload(filename string, format string, jsonValue any, csvBuilder func() ([][]string, []string)) (exportDownload, error) {
	if format == exportFormatJSON {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		if err := enc.Encode(jsonValue); err != nil {
			return exportDownload{}, err
		}
		return exportDownload{
			ContentType: "application/json; charset=utf-8",
			Filename:    filename,
			Body:        buf.Bytes(),
		}, nil
	}
	rows, header := csvBuilder()
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(header); err != nil {
		return exportDownload{}, err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return exportDownload{}, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return exportDownload{}, err
	}
	return exportDownload{
		ContentType: "text/csv; charset=utf-8",
		Filename:    filename,
		Body:        buf.Bytes(),
	}, nil
}

func buildExportFilename(prefix string, format string, profile string, runID string) string {
	suffix := profile
	if suffix == "" {
		suffix = exportProfileRaw
	}
	if runID != "" {
		suffix = suffix + "-" + sanitizeDownloadToken(runID)
	}
	return fmt.Sprintf("%s-%s.%s", prefix, sanitizeDownloadToken(suffix), format)
}

func sanitizeDownloadToken(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "export"
	}
	var b strings.Builder
	for _, r := range trimmed {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	cleaned := strings.Trim(b.String(), "-")
	if cleaned == "" {
		return "export"
	}
	return cleaned
}

func mapWorkDetailsForExport(rows []sqlcgen.ListWorkDetailsForExportRow) ([]detailExportRecord, error) {
	result := make([]detailExportRecord, 0, len(rows))
	for _, row := range rows {
		details, err := unmarshalWorkDetailExtras(row.DetailsJson)
		if err != nil {
			return nil, err
		}
		result = append(result, detailExportRecord{
			WorkHref:             row.WorkHref,
			RunID:                row.SourceRunID,
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
			RawDetailsJSON:       row.DetailsJson,
		})
	}
	return result, nil
}

func mapWorkDetailSnapshots(rows []sqlcgen.WorkDetailSnapshot) ([]detailExportRecord, error) {
	result := make([]detailExportRecord, 0, len(rows))
	for _, row := range rows {
		details, err := unmarshalWorkDetailExtras(row.DetailsJson)
		if err != nil {
			return nil, err
		}
		result = append(result, detailExportRecord{
			WorkHref:             row.WorkHref,
			RunID:                row.CrawlRunID,
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
			LastFetchedAt:        row.FetchedAt,
			Details:              details,
			RawDetailsJSON:       row.DetailsJson,
		})
	}
	return result, nil
}

func unmarshalWorkDetailExtras(raw string) (domain.WorkDetailExtras, error) {
	var details domain.WorkDetailExtras
	if raw == "" {
		raw = "{}"
	}
	if err := sonic.UnmarshalString(raw, &details); err != nil {
		return domain.WorkDetailExtras{}, err
	}
	return details, nil
}

func mapDetailExportRecordsToRaw(records []detailExportRecord) []detailView {
	result := make([]detailView, 0, len(records))
	for _, row := range records {
		details := map[string]any{}
		_ = json.Unmarshal([]byte(row.RawDetailsJSON), &details)
		result = append(result, detailView{
			WorkHref:             row.WorkHref,
			Category:             row.Category,
			Title:                row.Title,
			Summary:              row.Summary,
			ReleaseDate:          row.ReleaseDate,
			Metascore:            row.Metascore,
			MetascoreSentiment:   row.MetascoreSentiment,
			MetascoreReviewCount: row.MetascoreReviewCount,
			UserScore:            row.UserScore,
			UserScoreSentiment:   row.UserScoreSentiment,
			UserScoreCount:       row.UserScoreCount,
			Rating:               row.Rating,
			Duration:             row.Duration,
			Tagline:              row.Tagline,
			LastFetchedAt:        row.LastFetchedAt,
			Details:              details,
		})
	}
	return result
}

func mapDetailExportRecordsToFlat(records []detailExportRecord) []detailFlatView {
	result := make([]detailFlatView, 0, len(records))
	for _, row := range records {
		result = append(result, detailFlatView{
			RunID:                  row.RunID,
			WorkHref:               row.WorkHref,
			Category:               row.Category,
			Title:                  row.Title,
			ReleaseDate:            row.ReleaseDate,
			Metascore:              row.Metascore,
			UserScore:              row.UserScore,
			Rating:                 row.Rating,
			Duration:               row.Duration,
			Tagline:                row.Tagline,
			LastFetchedAt:          row.LastFetchedAt,
			GenresCSV:              joinCSVValues(row.Details.Genres),
			PlatformsCSV:           joinCSVValues(row.Details.Platforms),
			DevelopersCSV:          joinCSVValues(row.Details.Developers),
			PublishersCSV:          joinCSVValues(row.Details.Publishers),
			ProductionCompaniesCSV: joinCSVValues(row.Details.ProductionCompanies),
			DirectorsCSV:           joinCSVValues(row.Details.Directors),
			WritersCSV:             joinCSVValues(row.Details.Writers),
			AwardsCount:            len(row.Details.Awards),
			SeasonsCount:           len(row.Details.Seasons),
		})
	}
	return result
}

func summarizeDetailExportRecords(records []detailExportRecord) []detailSummaryView {
	type groupKey struct {
		runID    string
		category string
	}
	groups := make(map[groupKey]*detailSummaryView, len(records))
	for _, row := range records {
		key := groupKey{runID: row.RunID, category: row.Category}
		group, ok := groups[key]
		if !ok {
			group = &detailSummaryView{RunID: row.RunID, Category: row.Category}
			groups[key] = group
		}
		group.RowCount++
		if row.Metascore != "" {
			group.WithMetascoreCount++
		}
		if row.UserScore != "" {
			group.WithUserScoreCount++
		}
		if row.Rating != "" {
			group.WithRatingCount++
		}
		if row.Duration != "" {
			group.WithDurationCount++
		}
	}
	result := make([]detailSummaryView, 0, len(groups))
	for _, group := range groups {
		result = append(result, *group)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].RunID != result[j].RunID {
			return result[i].RunID < result[j].RunID
		}
		return result[i].Category < result[j].Category
	})
	return result
}

func mapLatestReviewsForExport(rows []sqlcgen.LatestReview) []reviewExportRecord {
	result := make([]reviewExportRecord, 0, len(rows))
	for _, row := range rows {
		result = append(result, reviewExportRecord{
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
			RunID:             row.CrawlRunID,
			LastCrawledAt:     row.CrawledAt,
		})
	}
	return result
}

func mapReviewExportRecordsToRaw(records []reviewExportRecord) []reviewView {
	result := make([]reviewView, 0, len(records))
	for _, row := range records {
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

func mapReviewExportRecordsToFlat(records []reviewExportRecord) []reviewFlatView {
	result := make([]reviewFlatView, 0, len(records))
	for _, row := range records {
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

func summarizeReviewExportRecords(records []reviewExportRecord) []reviewSummaryView {
	type groupKey struct {
		runID, category, reviewType, platformKey string
	}
	type aggregate struct {
		view       reviewSummaryView
		totalScore float64
	}
	groups := make(map[groupKey]*aggregate, len(records))
	for _, row := range records {
		key := groupKey{row.RunID, row.Category, row.ReviewType, row.PlatformKey}
		group, ok := groups[key]
		if !ok {
			group = &aggregate{view: reviewSummaryView{
				RunID: row.RunID, Category: row.Category, ReviewType: row.ReviewType, PlatformKey: row.PlatformKey,
			}}
			groups[key] = group
		}
		group.view.RowCount++
		if row.Score != "" {
			group.view.ScoredCount++
			if parsed, err := strconv.ParseFloat(row.Score, 64); err == nil {
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

func summarizeLatestEntries(entries []latestEntryView) []latestSummaryView {
	type groupKey struct {
		runID, category, metric, filterKey string
	}
	type aggregate struct {
		view  latestSummaryView
		works map[string]struct{}
	}
	groups := make(map[groupKey]*aggregate, len(entries))
	for _, entry := range entries {
		key := groupKey{entry.SourceCrawlRunID, entry.Category, entry.Metric, entry.FilterKey}
		group, ok := groups[key]
		if !ok {
			group = &aggregate{
				view: latestSummaryView{
					RunID: entry.SourceCrawlRunID, Category: entry.Category, Metric: entry.Metric, FilterKey: entry.FilterKey,
					MinRank: entry.RankNo, MaxRank: entry.RankNo,
				},
				works: make(map[string]struct{}),
			}
			groups[key] = group
		}
		group.view.RowCount++
		group.works[entry.WorkHref] = struct{}{}
		if entry.RankNo < group.view.MinRank {
			group.view.MinRank = entry.RankNo
		}
		if entry.RankNo > group.view.MaxRank {
			group.view.MaxRank = entry.RankNo
		}
	}
	result := make([]latestSummaryView, 0, len(groups))
	for _, group := range groups {
		group.view.DistinctWorkCount = len(group.works)
		result = append(result, group.view)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].RunID != result[j].RunID {
			return result[i].RunID < result[j].RunID
		}
		if result[i].Category != result[j].Category {
			return result[i].Category < result[j].Category
		}
		if result[i].Metric != result[j].Metric {
			return result[i].Metric < result[j].Metric
		}
		return result[i].FilterKey < result[j].FilterKey
	})
	return result
}

func mapSnapshotListEntries(entries []sqlcgen.ListEntry) []latestEntryView {
	result := make([]latestEntryView, 0, len(entries))
	for _, entry := range entries {
		result = append(result, latestEntryView{
			WorkHref:         entry.WorkHref,
			Category:         entry.Category,
			Metric:           entry.Metric,
			FilterKey:        entry.FilterKey,
			PageNo:           entry.PageNo,
			RankNo:           entry.RankNo,
			Metascore:        nullString(entry.Metascore),
			UserScore:        nullString(entry.UserScore),
			LastCrawledAt:    entry.CrawledAt,
			SourceCrawlRunID: entry.CrawlRunID,
		})
	}
	return result
}

func joinCSVValues(values []string) string {
	filtered := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		filtered = append(filtered, trimmed)
	}
	sort.Strings(filtered)
	return strings.Join(filtered, ", ")
}
