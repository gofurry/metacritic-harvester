package cli

import (
	"database/sql"
	"sort"

	"github.com/bytedance/sonic"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	sqlcgen "github.com/gofurry/metacritic-harvester/internal/storage/sqlcgen"
)

type detailView struct {
	WorkHref             string                  `json:"work_href"`
	Category             string                  `json:"category"`
	Title                string                  `json:"title"`
	Summary              string                  `json:"summary,omitempty"`
	ReleaseDate          string                  `json:"release_date,omitempty"`
	Metascore            string                  `json:"metascore,omitempty"`
	MetascoreSentiment   string                  `json:"metascore_sentiment,omitempty"`
	MetascoreReviewCount int                     `json:"metascore_review_count,omitempty"`
	UserScore            string                  `json:"user_score,omitempty"`
	UserScoreSentiment   string                  `json:"user_score_sentiment,omitempty"`
	UserScoreCount       int                     `json:"user_score_count,omitempty"`
	Rating               string                  `json:"rating,omitempty"`
	Duration             string                  `json:"duration,omitempty"`
	Tagline              string                  `json:"tagline,omitempty"`
	LastFetchedAt        string                  `json:"last_fetched_at"`
	Details              domain.WorkDetailExtras `json:"details"`
	RawDetailsJSON       string                  `json:"-"`
}

type detailCompareView struct {
	WorkHref           string `json:"work_href"`
	Category           string `json:"category"`
	ChangeType         string `json:"change_type"`
	FromTitle          string `json:"from_title,omitempty"`
	ToTitle            string `json:"to_title,omitempty"`
	FromReleaseDate    string `json:"from_release_date,omitempty"`
	ToReleaseDate      string `json:"to_release_date,omitempty"`
	FromMetascore      string `json:"from_metascore,omitempty"`
	ToMetascore        string `json:"to_metascore,omitempty"`
	FromUserScore      string `json:"from_user_score,omitempty"`
	ToUserScore        string `json:"to_user_score,omitempty"`
	FromRating         string `json:"from_rating,omitempty"`
	ToRating           string `json:"to_rating,omitempty"`
	FromDuration       string `json:"from_duration,omitempty"`
	ToDuration         string `json:"to_duration,omitempty"`
	FromTagline        string `json:"from_tagline,omitempty"`
	ToTagline          string `json:"to_tagline,omitempty"`
	DetailsJSONChanged bool   `json:"details_json_changed"`
	FromDetailsJSON    string `json:"from_details_json,omitempty"`
	ToDetailsJSON      string `json:"to_details_json,omitempty"`
}

func mapWorkDetails(rows []sqlcgen.WorkDetail) ([]detailView, error) {
	result := make([]detailView, 0, len(rows))
	for _, row := range rows {
		details, err := unmarshalWorkDetailExtras(row.DetailsJson)
		if err != nil {
			return nil, err
		}
		result = append(result, detailView{
			WorkHref:             row.WorkHref,
			Category:             row.Category,
			Title:                row.Title,
			Summary:              nullStringValue(row.Summary),
			ReleaseDate:          nullStringValue(row.ReleaseDate),
			Metascore:            nullStringValue(row.Metascore),
			MetascoreSentiment:   nullStringValue(row.MetascoreSentiment),
			MetascoreReviewCount: nullIntValue(row.MetascoreReviewCount),
			UserScore:            nullStringValue(row.UserScore),
			UserScoreSentiment:   nullStringValue(row.UserScoreSentiment),
			UserScoreCount:       nullIntValue(row.UserScoreCount),
			Rating:               nullStringValue(row.Rating),
			Duration:             nullStringValue(row.Duration),
			Tagline:              nullStringValue(row.Tagline),
			LastFetchedAt:        row.LastFetchedAt,
			Details:              details,
			RawDetailsJSON:       row.DetailsJson,
		})
	}
	return result, nil
}

func mapDetailCompareRows(rows []sqlcgen.CompareWorkDetailSnapshotsRow) []detailCompareView {
	result := make([]detailCompareView, 0, len(rows))
	for _, row := range rows {
		result = append(result, detailCompareView{
			WorkHref:           row.WorkHref,
			Category:           row.Category,
			ChangeType:         row.ChangeType,
			FromTitle:          interfaceValueString(row.FromTitle),
			ToTitle:            nullStringValue(row.ToTitle),
			FromReleaseDate:    nullStringValue(row.FromReleaseDate),
			ToReleaseDate:      nullStringValue(row.ToReleaseDate),
			FromMetascore:      nullStringValue(row.FromMetascore),
			ToMetascore:        nullStringValue(row.ToMetascore),
			FromUserScore:      nullStringValue(row.FromUserScore),
			ToUserScore:        nullStringValue(row.ToUserScore),
			FromRating:         nullStringValue(row.FromRating),
			ToRating:           nullStringValue(row.ToRating),
			FromDuration:       nullStringValue(row.FromDuration),
			ToDuration:         nullStringValue(row.ToDuration),
			FromTagline:        nullStringValue(row.FromTagline),
			ToTagline:          nullStringValue(row.ToTagline),
			DetailsJSONChanged: row.DetailsJsonChanged != 0,
			FromDetailsJSON:    interfaceValueString(row.FromDetailsJson),
			ToDetailsJSON:      nullStringValue(row.ToDetailsJson),
		})
	}
	return result
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

func nullIntValue(value sql.NullInt64) int {
	if !value.Valid {
		return 0
	}
	return int(value.Int64)
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
			Summary:              nullStringValue(row.Summary),
			ReleaseDate:          nullStringValue(row.ReleaseDate),
			Metascore:            nullStringValue(row.Metascore),
			MetascoreSentiment:   nullStringValue(row.MetascoreSentiment),
			MetascoreReviewCount: nullIntValue(row.MetascoreReviewCount),
			UserScore:            nullStringValue(row.UserScore),
			UserScoreSentiment:   nullStringValue(row.UserScoreSentiment),
			UserScoreCount:       nullIntValue(row.UserScoreCount),
			Rating:               nullStringValue(row.Rating),
			Duration:             nullStringValue(row.Duration),
			Tagline:              nullStringValue(row.Tagline),
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
			Summary:              nullStringValue(row.Summary),
			ReleaseDate:          nullStringValue(row.ReleaseDate),
			Metascore:            nullStringValue(row.Metascore),
			MetascoreSentiment:   nullStringValue(row.MetascoreSentiment),
			MetascoreReviewCount: nullIntValue(row.MetascoreReviewCount),
			UserScore:            nullStringValue(row.UserScore),
			UserScoreSentiment:   nullStringValue(row.UserScoreSentiment),
			UserScoreCount:       nullIntValue(row.UserScoreCount),
			Rating:               nullStringValue(row.Rating),
			Duration:             nullStringValue(row.Duration),
			Tagline:              nullStringValue(row.Tagline),
			LastFetchedAt:        row.FetchedAt,
			Details:              details,
			RawDetailsJSON:       row.DetailsJson,
		})
	}
	return result, nil
}

func mapDetailExportRecordsToRaw(records []detailExportRecord) []detailView {
	result := make([]detailView, 0, len(records))
	for _, row := range records {
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
			Details:              row.Details,
			RawDetailsJSON:       row.RawDetailsJSON,
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
			group = &detailSummaryView{
				RunID:    row.RunID,
				Category: row.Category,
			}
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
