package cli

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/bytedance/sonic"
	"github.com/spf13/pflag"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
	sqlcgen "github.com/gofurry/metacritic-harvester/internal/storage/sqlcgen"
)

func openRepository(ctx context.Context, dbPath string, checkpoint bool) (*storage.Repository, func() error, error) {
	db, err := storage.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return nil, nil, err
	}
	return storage.NewRepository(db), func() error {
		if err := db.Close(); err != nil {
			return err
		}
		if checkpoint {
			return storage.Checkpoint(ctx, dbPath)
		}
		return nil
	}, nil
}

func finishReadRepository(runErr error, closeFn func() error) error {
	if closeFn == nil {
		return runErr
	}
	closeErr := closeFn()
	if runErr != nil {
		if closeErr != nil {
			return fmt.Errorf("%w (cleanup: %v)", runErr, closeErr)
		}
		return runErr
	}
	return closeErr
}

func scopePart(key string, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return fmt.Sprintf("%s=%s", key, value)
}

func scopePartInt(key string, value int) string {
	if value == 0 {
		return ""
	}
	return fmt.Sprintf("%s=%d", key, value)
}

func buildReadScope(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		filtered = append(filtered, part)
	}
	return strings.Join(filtered, " ")
}

func wrapReadCommandError(command string, err error, parts ...string) error {
	if err == nil {
		return nil
	}
	scope := buildReadScope(parts...)
	if scope == "" {
		return fmt.Errorf("%s failed: %w", command, err)
	}
	return fmt.Errorf("%s failed (%s): %w", command, scope, err)
}

func addCheckpointFlag(cmd interface {
	Flags() *pflag.FlagSet
}, target *bool) {
	cmd.Flags().BoolVar(target, "checkpoint", false, "Run PRAGMA wal_checkpoint(TRUNCATE) after the command finishes")
}

func validateOptionalCategoryMetric(category string, metric string) error {
	if strings.TrimSpace(category) != "" {
		if _, err := domain.ParseCategory(category); err != nil {
			return err
		}
	}
	if strings.TrimSpace(metric) != "" {
		if _, err := domain.ParseMetric(metric); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(w io.Writer, value any) error {
	content, err := sonic.Marshal(value)
	if err != nil {
		return err
	}
	_, err = w.Write(append(content, '\n'))
	return err
}

func createOutputFile(path string) (*os.File, error) {
	cleaned := filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(cleaned), 0o755); err != nil {
		return nil, err
	}
	return os.Create(cleaned)
}

func writeCSV(w io.Writer, header []string, rows [][]string) error {
	writer := csv.NewWriter(w)
	if err := writer.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func newTabWriter(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullInt64Value(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	return strconv.FormatInt(value.Int64, 10)
}

func nullFloat64Value(value sql.NullFloat64) string {
	if !value.Valid {
		return ""
	}
	return strconv.FormatFloat(value.Float64, 'f', -1, 64)
}

func nullBoolIntValue(value sql.NullInt64) string {
	if !value.Valid {
		return ""
	}
	if value.Int64 != 0 {
		return "true"
	}
	return "false"
}

func interfaceValueString(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
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

func mapLatestEntries(entries []sqlcgen.LatestListEntry) []latestEntryView {
	result := make([]latestEntryView, 0, len(entries))
	for _, entry := range entries {
		result = append(result, latestEntryView{
			WorkHref:         entry.WorkHref,
			Category:         entry.Category,
			Metric:           entry.Metric,
			FilterKey:        entry.FilterKey,
			PageNo:           entry.PageNo,
			RankNo:           entry.RankNo,
			Metascore:        nullStringValue(entry.Metascore),
			UserScore:        nullStringValue(entry.UserScore),
			LastCrawledAt:    entry.LastCrawledAt,
			SourceCrawlRunID: entry.SourceCrawlRunID,
		})
	}
	return result
}

func mapListEntries(entries []sqlcgen.ListEntry) []latestEntryView {
	result := make([]latestEntryView, 0, len(entries))
	for _, entry := range entries {
		result = append(result, latestEntryView{
			WorkHref:         entry.WorkHref,
			Category:         entry.Category,
			Metric:           entry.Metric,
			FilterKey:        entry.FilterKey,
			PageNo:           entry.PageNo,
			RankNo:           entry.RankNo,
			Metascore:        nullStringValue(entry.Metascore),
			UserScore:        nullStringValue(entry.UserScore),
			LastCrawledAt:    entry.CrawledAt,
			SourceCrawlRunID: entry.CrawlRunID,
		})
	}
	return result
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

func summarizeLatestEntries(entries []latestEntryView) []latestSummaryView {
	type groupKey struct {
		runID     string
		category  string
		metric    string
		filterKey string
	}
	type aggregate struct {
		view  latestSummaryView
		works map[string]struct{}
	}

	groups := make(map[groupKey]*aggregate, len(entries))
	for _, entry := range entries {
		key := groupKey{
			runID:     entry.SourceCrawlRunID,
			category:  entry.Category,
			metric:    entry.Metric,
			filterKey: entry.FilterKey,
		}
		group, ok := groups[key]
		if !ok {
			group = &aggregate{
				view: latestSummaryView{
					RunID:     entry.SourceCrawlRunID,
					Category:  entry.Category,
					Metric:    entry.Metric,
					FilterKey: entry.FilterKey,
					MinRank:   entry.RankNo,
					MaxRank:   entry.RankNo,
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

type compareRowView struct {
	WorkHref      string `json:"work_href"`
	Category      string `json:"category"`
	Metric        string `json:"metric"`
	FilterKey     string `json:"filter_key"`
	FromRank      string `json:"from_rank,omitempty"`
	ToRank        string `json:"to_rank,omitempty"`
	RankDiff      string `json:"rank_diff,omitempty"`
	FromMetascore string `json:"from_metascore,omitempty"`
	ToMetascore   string `json:"to_metascore,omitempty"`
	MetascoreDiff string `json:"metascore_diff,omitempty"`
	FromUserScore string `json:"from_user_score,omitempty"`
	ToUserScore   string `json:"to_user_score,omitempty"`
	UserScoreDiff string `json:"user_score_diff,omitempty"`
	ChangeType    string `json:"change_type"`
}

func mapCompareRows(rows []sqlcgen.CompareCrawlRunsRow) []compareRowView {
	result := make([]compareRowView, 0, len(rows))
	for _, row := range rows {
		result = append(result, compareRowView{
			WorkHref:      row.WorkHref,
			Category:      row.Category,
			Metric:        row.Metric,
			FilterKey:     row.FilterKey,
			FromRank:      interfaceValueString(row.FromRank),
			ToRank:        nullInt64Value(row.ToRank),
			RankDiff:      interfaceValueString(row.RankDiff),
			FromMetascore: nullStringValue(row.FromMetascore),
			ToMetascore:   nullStringValue(row.ToMetascore),
			MetascoreDiff: interfaceValueString(row.MetascoreDiff),
			FromUserScore: nullStringValue(row.FromUserScore),
			ToUserScore:   nullStringValue(row.ToUserScore),
			UserScoreDiff: interfaceValueString(row.UserScoreDiff),
			ChangeType:    row.ChangeType,
		})
	}
	return result
}
