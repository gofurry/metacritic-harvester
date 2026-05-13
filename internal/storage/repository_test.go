package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestRepositoryUpsertAndInsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	work := domain.Work{
		Name:        "Test Game",
		Href:        "https://www.metacritic.com/game/test-game",
		ImageURL:    "https://www.metacritic.com/images/test-game.jpg",
		ReleaseDate: "Apr 23, 2026",
		Category:    "game",
	}

	if err := repo.UpsertWork(ctx, work); err != nil {
		t.Fatalf("first UpsertWork() error = %v", err)
	}

	work.Name = "Updated Test Game"
	if err := repo.UpsertWork(ctx, work); err != nil {
		t.Fatalf("second UpsertWork() error = %v", err)
	}

	firstTask := domain.ListTask{
		Category: domain.CategoryGame,
		Metric:   domain.MetricMetascore,
	}
	secondTask := firstTask
	if err := repo.CreateCrawlRun(ctx, "run-1", "crawl list", "game-metascore-1", firstTask, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun(run-1) error = %v", err)
	}
	if err := repo.CreateCrawlRun(ctx, "run-2", "crawl list", "game-metascore-2", secondTask, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun(run-2) error = %v", err)
	}

	entry1 := domain.ListEntry{
		CrawlRunID: "run-1",
		WorkHref:   work.Href,
		Category:   "game",
		Metric:     "metascore",
		Page:       1,
		Rank:       1,
		Metascore:  "95",
		UserScore:  "8.7",
		FilterKey:  "|||",
		CrawledAt:  time.Now().UTC(),
	}
	entry2 := entry1
	entry2.CrawlRunID = "run-2"
	entry2.Page = 2
	entry2.Rank = 2
	entry2.Metascore = "96"

	if err := repo.InsertListEntry(ctx, entry1); err != nil {
		t.Fatalf("InsertListEntry(entry1) error = %v", err)
	}
	if err := repo.UpsertLatestListEntry(ctx, entry1); err != nil {
		t.Fatalf("UpsertLatestListEntry(entry1) error = %v", err)
	}
	if err := repo.InsertListEntry(ctx, entry2); err != nil {
		t.Fatalf("InsertListEntry(entry2) error = %v", err)
	}
	if err := repo.UpsertLatestListEntry(ctx, entry2); err != nil {
		t.Fatalf("UpsertLatestListEntry(entry2) error = %v", err)
	}

	gotWork, err := repo.GetWorkByHref(ctx, work.Href)
	if err != nil {
		t.Fatalf("GetWorkByHref() error = %v", err)
	}
	if gotWork.Name != "Updated Test Game" {
		t.Fatalf("expected updated name, got %q", gotWork.Name)
	}

	workCount, err := repo.CountWorks(ctx)
	if err != nil {
		t.Fatalf("CountWorks() error = %v", err)
	}
	if workCount != 1 {
		t.Fatalf("expected 1 work, got %d", workCount)
	}

	entryCount, err := repo.CountListEntries(ctx)
	if err != nil {
		t.Fatalf("CountListEntries() error = %v", err)
	}
	if entryCount != 2 {
		t.Fatalf("expected 2 list entries, got %d", entryCount)
	}

	latestCount, err := repo.CountLatestListEntries(ctx)
	if err != nil {
		t.Fatalf("CountLatestListEntries() error = %v", err)
	}
	if latestCount != 1 {
		t.Fatalf("expected 1 latest list entry, got %d", latestCount)
	}

	latestEntry, err := repo.GetLatestListEntry(ctx, entry1)
	if err != nil {
		t.Fatalf("GetLatestListEntry() error = %v", err)
	}
	if latestEntry.PageNo != 2 || latestEntry.RankNo != 2 {
		t.Fatalf("expected latest page/rank 2/2, got %d/%d", latestEntry.PageNo, latestEntry.RankNo)
	}
	if !latestEntry.Metascore.Valid || latestEntry.Metascore.String != "96" {
		t.Fatalf("expected latest metascore 96, got %+v", latestEntry.Metascore)
	}
	if latestEntry.SourceCrawlRunID != "run-2" {
		t.Fatalf("expected latest source run id run-2, got %q", latestEntry.SourceCrawlRunID)
	}

	runCount, err := repo.CountCrawlRuns(ctx)
	if err != nil {
		t.Fatalf("CountCrawlRuns() error = %v", err)
	}
	if runCount != 2 {
		t.Fatalf("expected 2 crawl runs, got %d", runCount)
	}
	runs, err := repo.ListCrawlRuns(ctx, 10)
	if err != nil {
		t.Fatalf("ListCrawlRuns() error = %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 crawl runs from list, got %d", len(runs))
	}
	latestRows, err := repo.ListLatestEntries(ctx, ListLatestEntriesFilter{
		Category: "game",
		Metric:   "metascore",
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("ListLatestEntries() error = %v", err)
	}
	if len(latestRows) != 1 {
		t.Fatalf("expected 1 latest row, got %d", len(latestRows))
	}
	compareRows, err := repo.CompareCrawlRuns(ctx, CompareRunsFilter{
		FromRunID: "run-1",
		ToRunID:   "run-2",
	})
	if err != nil {
		t.Fatalf("CompareCrawlRuns() error = %v", err)
	}
	if len(compareRows) != 1 {
		t.Fatalf("expected 1 compare row, got %d", len(compareRows))
	}
	if compareRows[0].ChangeType != "changed" {
		t.Fatalf("expected change_type changed, got %q", compareRows[0].ChangeType)
	}
}

func TestRepositorySaveListEntrySnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "snapshot.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	task := domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore}
	if err := repo.CreateCrawlRun(ctx, "run-snapshot", "crawl list", "snapshot", task, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun() error = %v", err)
	}

	work := domain.Work{
		Name:     "Snapshot Game",
		Href:     "https://www.metacritic.com/game/snapshot",
		Category: domain.CategoryGame,
	}
	entry := domain.ListEntry{
		CrawlRunID: "run-snapshot",
		WorkHref:   work.Href,
		Category:   domain.CategoryGame,
		Metric:     domain.MetricMetascore,
		Page:       1,
		Rank:       1,
		Metascore:  "91",
		FilterKey:  task.Filter.Key(),
		CrawledAt:  time.Now().UTC(),
	}

	if err := repo.SaveListEntrySnapshot(ctx, work, entry); err != nil {
		t.Fatalf("SaveListEntrySnapshot() error = %v", err)
	}

	workCount, err := repo.CountWorks(ctx)
	if err != nil {
		t.Fatalf("CountWorks() error = %v", err)
	}
	entryCount, err := repo.CountListEntries(ctx)
	if err != nil {
		t.Fatalf("CountListEntries() error = %v", err)
	}
	latestCount, err := repo.CountLatestListEntries(ctx)
	if err != nil {
		t.Fatalf("CountLatestListEntries() error = %v", err)
	}
	if workCount != 1 || entryCount != 1 || latestCount != 1 {
		t.Fatalf("expected 1/1/1 rows, got works=%d entries=%d latest=%d", workCount, entryCount, latestCount)
	}
}

func TestRepositorySaveListEntrySnapshots(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "snapshots.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	task := domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore}
	if err := repo.CreateCrawlRun(ctx, "run-snapshots", "crawl list", "snapshots", task, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun() error = %v", err)
	}

	snapshots := []ListEntrySnapshot{
		testSnapshot("run-snapshots", "alpha", 1),
		testSnapshot("run-snapshots", "beta", 2),
	}
	if err := repo.SaveListEntrySnapshots(ctx, snapshots); err != nil {
		t.Fatalf("SaveListEntrySnapshots() error = %v", err)
	}

	workCount, err := repo.CountWorks(ctx)
	if err != nil {
		t.Fatalf("CountWorks() error = %v", err)
	}
	entryCount, err := repo.CountListEntries(ctx)
	if err != nil {
		t.Fatalf("CountListEntries() error = %v", err)
	}
	latestCount, err := repo.CountLatestListEntries(ctx)
	if err != nil {
		t.Fatalf("CountLatestListEntries() error = %v", err)
	}
	if workCount != 2 || entryCount != 2 || latestCount != 2 {
		t.Fatalf("expected 2/2/2 rows, got works=%d entries=%d latest=%d", workCount, entryCount, latestCount)
	}
}

func TestRepositorySaveListEntrySnapshotRollsBack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "rollback.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	work := domain.Work{
		Name:     "Rollback Game",
		Href:     "https://www.metacritic.com/game/rollback",
		Category: domain.CategoryGame,
	}
	entry := domain.ListEntry{
		CrawlRunID: "missing-run",
		WorkHref:   work.Href,
		Category:   domain.CategoryGame,
		Metric:     domain.MetricMetascore,
		Page:       1,
		Rank:       1,
		Metascore:  "91",
		FilterKey:  domain.Filter{}.Key(),
		CrawledAt:  time.Now().UTC(),
	}

	if err := repo.SaveListEntrySnapshot(ctx, work, entry); err == nil {
		t.Fatal("expected SaveListEntrySnapshot() to fail")
	}

	workCount, err := repo.CountWorks(ctx)
	if err != nil {
		t.Fatalf("CountWorks() error = %v", err)
	}
	entryCount, err := repo.CountListEntries(ctx)
	if err != nil {
		t.Fatalf("CountListEntries() error = %v", err)
	}
	latestCount, err := repo.CountLatestListEntries(ctx)
	if err != nil {
		t.Fatalf("CountLatestListEntries() error = %v", err)
	}
	if workCount != 0 || entryCount != 0 || latestCount != 0 {
		t.Fatalf("expected rollback to leave 0/0/0 rows, got works=%d entries=%d latest=%d", workCount, entryCount, latestCount)
	}
}

func TestRepositorySaveListEntrySnapshotsRollsBack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "snapshots-rollback.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	if err := repo.CreateCrawlRun(ctx, "run-valid", "crawl list", "valid", domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore}, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun() error = %v", err)
	}

	snapshots := []ListEntrySnapshot{
		testSnapshot("run-valid", "alpha", 1),
		testSnapshot("missing-run", "beta", 2),
	}
	if err := repo.SaveListEntrySnapshots(ctx, snapshots); err == nil {
		t.Fatal("expected SaveListEntrySnapshots() to fail")
	}

	workCount, err := repo.CountWorks(ctx)
	if err != nil {
		t.Fatalf("CountWorks() error = %v", err)
	}
	entryCount, err := repo.CountListEntries(ctx)
	if err != nil {
		t.Fatalf("CountListEntries() error = %v", err)
	}
	latestCount, err := repo.CountLatestListEntries(ctx)
	if err != nil {
		t.Fatalf("CountLatestListEntries() error = %v", err)
	}
	if workCount != 0 || entryCount != 0 || latestCount != 0 {
		t.Fatalf("expected rollback to leave 0/0/0 rows, got works=%d entries=%d latest=%d", workCount, entryCount, latestCount)
	}
}

func TestSchemaCreatesCompareIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "PRAGMA index_info(idx_list_entries_compare)")
	if err != nil {
		t.Fatalf("index_info() error = %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var seqno, cid int
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			t.Fatalf("Scan() error = %v", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error = %v", err)
	}

	want := []string{"crawl_run_id", "category", "metric", "work_href", "filter_key"}
	if len(columns) != len(want) {
		t.Fatalf("expected index columns %v, got %v", want, columns)
	}
	for i := range want {
		if columns[i] != want[i] {
			t.Fatalf("expected index columns %v, got %v", want, columns)
		}
	}
}

func TestSchemaBackfillsLegacyRunLineage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	oldDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := oldDB.ExecContext(ctx, `
CREATE TABLE works (
    href TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    image_url TEXT,
    release_date TEXT,
    category TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE list_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    work_href TEXT NOT NULL,
    category TEXT NOT NULL,
    metric TEXT NOT NULL,
    page_no INTEGER NOT NULL,
    rank_no INTEGER NOT NULL,
    metascore TEXT,
    user_score TEXT,
    filter_key TEXT NOT NULL,
    crawled_at TEXT NOT NULL
);
CREATE TABLE latest_list_entries (
    work_href TEXT NOT NULL,
    category TEXT NOT NULL,
    metric TEXT NOT NULL,
    filter_key TEXT NOT NULL,
    page_no INTEGER NOT NULL,
    rank_no INTEGER NOT NULL,
    metascore TEXT,
    user_score TEXT,
    last_crawled_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (work_href, category, metric, filter_key)
);
INSERT INTO works (href, name, category) VALUES ('https://www.metacritic.com/game/legacy', 'Legacy', 'game');
INSERT INTO list_entries (work_href, category, metric, page_no, rank_no, metascore, filter_key, crawled_at)
VALUES ('https://www.metacritic.com/game/legacy', 'game', 'metascore', 1, 1, '90', '|||', '2026-04-23T00:00:00Z');
INSERT INTO latest_list_entries (work_href, category, metric, filter_key, page_no, rank_no, metascore, last_crawled_at)
VALUES ('https://www.metacritic.com/game/legacy', 'game', 'metascore', '|||', 1, 1, '90', '2026-04-23T00:00:00Z');
`); err != nil {
		_ = oldDB.Close()
		t.Fatalf("seed old schema error = %v", err)
	}
	if err := oldDB.Close(); err != nil {
		t.Fatalf("close old db error = %v", err)
	}

	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	run, err := repo.GetCrawlRun(ctx, legacyUpgradeRunID)
	if err != nil {
		t.Fatalf("GetCrawlRun(%s) error = %v", legacyUpgradeRunID, err)
	}
	if run.Source != "schema upgrade" || run.Status != "completed" {
		t.Fatalf("unexpected legacy run: %+v", run)
	}

	var listRunID string
	if err := db.QueryRowContext(ctx, "SELECT crawl_run_id FROM list_entries").Scan(&listRunID); err != nil {
		t.Fatalf("query list run id error = %v", err)
	}
	var latestRunID string
	if err := db.QueryRowContext(ctx, "SELECT source_crawl_run_id FROM latest_list_entries").Scan(&latestRunID); err != nil {
		t.Fatalf("query latest run id error = %v", err)
	}
	if listRunID != legacyUpgradeRunID || latestRunID != legacyUpgradeRunID {
		t.Fatalf("expected legacy run ids, got list=%q latest=%q", listRunID, latestRunID)
	}
}

func TestRepositoryCompareCrawlRunsAddedEntry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "compare.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	task := domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore}
	if err := repo.CreateCrawlRun(ctx, "run-a", "crawl list", "task-a", task, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun(run-a) error = %v", err)
	}
	if err := repo.CreateCrawlRun(ctx, "run-b", "crawl list", "task-b", task, time.Now().UTC()); err != nil {
		t.Fatalf("CreateCrawlRun(run-b) error = %v", err)
	}

	work := domain.Work{
		Name:     "Gamma",
		Href:     "https://www.metacritic.com/game/gamma",
		Category: domain.CategoryGame,
	}
	if err := repo.UpsertWork(ctx, work); err != nil {
		t.Fatalf("UpsertWork() error = %v", err)
	}
	if err := repo.InsertListEntry(ctx, domain.ListEntry{
		CrawlRunID: "run-b",
		WorkHref:   work.Href,
		Category:   domain.CategoryGame,
		Metric:     domain.MetricMetascore,
		Page:       1,
		Rank:       3,
		Metascore:  "88",
		FilterKey:  task.Filter.Key(),
		CrawledAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("InsertListEntry() error = %v", err)
	}

	rows, err := repo.CompareCrawlRuns(ctx, CompareRunsFilter{
		FromRunID: "run-a",
		ToRunID:   "run-b",
	})
	if err != nil {
		t.Fatalf("CompareCrawlRuns() error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 compare row, got %d", len(rows))
	}
	if rows[0].ChangeType != "added" {
		t.Fatalf("expected added change type, got %q", rows[0].ChangeType)
	}
	if got := rows[0].FromRank; got == nil || got != int64(0) {
		t.Fatalf("expected added row from_rank 0, got %#v", rows[0].FromRank)
	}
}

func testSnapshot(runID string, slug string, rank int) ListEntrySnapshot {
	href := "https://www.metacritic.com/game/" + slug
	return ListEntrySnapshot{
		Work: domain.Work{
			Name:     slug,
			Href:     href,
			Category: domain.CategoryGame,
		},
		Entry: domain.ListEntry{
			CrawlRunID: runID,
			WorkHref:   href,
			Category:   domain.CategoryGame,
			Metric:     domain.MetricMetascore,
			Page:       1,
			Rank:       rank,
			Metascore:  "90",
			FilterKey:  domain.Filter{}.Key(),
			CrawledAt:  time.Now().UTC(),
		},
	}
}
