package serve

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gofurry/metacritic-harvester/internal/storage"
)

type fetchStateSummaryView struct {
	Total      int            `json:"total"`
	ByStatus   map[string]int `json:"by_status"`
	LastFailed int            `json:"last_failed"`
}

type exportCapabilityView struct {
	Kind     string   `json:"kind"`
	Formats  []string `json:"formats"`
	Profiles []string `json:"profiles"`
}

type overviewView struct {
	Runs         []crawlRunView         `json:"runs"`
	Tasks        []TaskView             `json:"tasks"`
	DetailStates fetchStateSummaryView  `json:"detail_states"`
	ReviewStates fetchStateSummaryView  `json:"review_states"`
	FailedRuns   int                    `json:"failed_runs"`
	Exports      []exportCapabilityView `json:"exports"`
}

func buildOverview(ctx context.Context, dbPath string, tasks []TaskView) (overviewView, error) {
	db, err := storage.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return overviewView{}, err
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	runs, err := repo.ListCrawlRuns(ctx, 20)
	if err != nil {
		return overviewView{}, err
	}
	detailStates, err := summarizeFetchStates(ctx, db, "detail_fetch_state")
	if err != nil {
		return overviewView{}, fmt.Errorf("summarize detail fetch state: %w", err)
	}
	reviewStates, err := summarizeFetchStates(ctx, db, "review_fetch_state")
	if err != nil {
		return overviewView{}, fmt.Errorf("summarize review fetch state: %w", err)
	}
	failedRuns := 0
	for _, row := range runs {
		if row.Status == "failed" {
			failedRuns++
		}
	}

	return overviewView{
		Runs:         mapCrawlRuns(runs),
		Tasks:        tasks,
		DetailStates: detailStates,
		ReviewStates: reviewStates,
		FailedRuns:   failedRuns,
		Exports: []exportCapabilityView{
			{Kind: "latest", Formats: []string{"csv", "json"}, Profiles: []string{"raw", "flat", "summary"}},
			{Kind: "detail", Formats: []string{"csv", "json"}, Profiles: []string{"raw", "flat", "summary"}},
			{Kind: "review", Formats: []string{"csv", "json"}, Profiles: []string{"raw", "flat", "summary"}},
		},
	}, nil
}

func summarizeFetchStates(ctx context.Context, db *sql.DB, table string) (fetchStateSummaryView, error) {
	query := fmt.Sprintf("SELECT status, COUNT(*) FROM %s GROUP BY status", table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fetchStateSummaryView{}, err
	}
	defer rows.Close()

	result := fetchStateSummaryView{ByStatus: map[string]int{}}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return fetchStateSummaryView{}, err
		}
		result.ByStatus[status] = count
		result.Total += count
		if status == "failed" {
			result.LastFailed = count
		}
	}
	if err := rows.Err(); err != nil {
		return fetchStateSummaryView{}, err
	}
	return result, nil
}
