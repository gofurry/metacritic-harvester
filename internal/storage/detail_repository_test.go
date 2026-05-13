package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	assets "github.com/gofurry/metacritic-harvester"
	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestSchemaCreatesWorkDetailsAndDetailStateMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-schema.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_attempted_at")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_run_id")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_error_type")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_error_stage")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "updated_at")
	assertHasTable(t, ctx, db, "work_detail_snapshots")

	var indexName string
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_detail_fetch_state_last_attempted_at'").Scan(&indexName); err != nil {
		t.Fatalf("expected detail_fetch_state index to exist: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_work_detail_snapshots_crawl_run_id_work_href'").Scan(&indexName); err != nil {
		t.Fatalf("expected work_detail_snapshots crawl run index to exist: %v", err)
	}
}

func TestRepositorySaveWorkDetailWritesSucceededState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	work := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/detail-alpha", "Old Name", domain.CategoryGame)
	createDetailRunForTest(t, ctx, repo, "detail-run-1", "game")
	attemptedAt := time.Date(2026, 4, 24, 9, 0, 0, 0, time.UTC)
	fetchedAt := attemptedAt.Add(2 * time.Minute)
	detail := domain.WorkDetail{
		WorkHref:             work.Href,
		Category:             domain.CategoryGame,
		Title:                "New Name",
		Summary:              "A useful summary.",
		ReleaseDate:          "Aug 3, 2023",
		Metascore:            "96",
		MetascoreSentiment:   "Universal Acclaim",
		MetascoreReviewCount: 119,
		Rating:               "Rated M",
		Details: domain.WorkDetailExtras{
			Genres: []string{"Western RPG"},
		},
		LastFetchedAt: fetchedAt,
	}

	if err := repo.SaveWorkDetail(ctx, detail, attemptedAt, "detail-run-1"); err != nil {
		t.Fatalf("SaveWorkDetail() error = %v", err)
	}

	row, err := repo.GetWorkDetail(ctx, work.Href)
	if err != nil {
		t.Fatalf("GetWorkDetail() error = %v", err)
	}
	if row.Title != "New Name" || !row.MetascoreReviewCount.Valid || row.MetascoreReviewCount.Int64 != 119 {
		t.Fatalf("unexpected detail row: %+v", row)
	}
	if !strings.Contains(row.DetailsJson, "Western RPG") {
		t.Fatalf("expected details json to include genre, got %q", row.DetailsJson)
	}
	snapshotCount, err := repo.CountWorkDetailSnapshots(ctx)
	if err != nil {
		t.Fatalf("CountWorkDetailSnapshots() error = %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected 1 snapshot row, got %d", snapshotCount)
	}
	perWorkCount, err := repo.CountWorkDetailSnapshotsByWorkHref(ctx, work.Href)
	if err != nil {
		t.Fatalf("CountWorkDetailSnapshotsByWorkHref() error = %v", err)
	}
	if perWorkCount != 1 {
		t.Fatalf("expected 1 snapshot row for work, got %d", perWorkCount)
	}

	updatedWork, err := repo.GetWorkByHref(ctx, work.Href)
	if err != nil {
		t.Fatalf("GetWorkByHref() error = %v", err)
	}
	if updatedWork.Name != "New Name" || !updatedWork.ReleaseDate.Valid || updatedWork.ReleaseDate.String != "Aug 3, 2023" {
		t.Fatalf("expected works row to be updated, got %+v", updatedWork)
	}

	state, err := repo.GetDetailFetchState(ctx, work.Href)
	if err != nil {
		t.Fatalf("GetDetailFetchState() error = %v", err)
	}
	if state.Status != DetailFetchStatusSucceeded {
		t.Fatalf("unexpected status: %+v", state)
	}
	if !state.LastAttemptedAt.Valid || state.LastAttemptedAt.String != attemptedAt.Format(time.RFC3339) {
		t.Fatalf("unexpected last_attempted_at: %+v", state)
	}
	if !state.LastFetchedAt.Valid || state.LastFetchedAt.String != fetchedAt.Format(time.RFC3339) {
		t.Fatalf("unexpected last_fetched_at: %+v", state)
	}
	if !state.LastRunID.Valid || state.LastRunID.String != "detail-run-1" {
		t.Fatalf("unexpected last_run_id: %+v", state)
	}
	if state.LastError.Valid || state.LastErrorType.Valid || state.LastErrorStage.Valid || state.UpdatedAt == "" {
		t.Fatalf("unexpected success state metadata: %+v", state)
	}
}

func TestRepositorySaveWorkDetailDeduplicatesSnapshotPerRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-snapshot-dedupe.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	work := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/detail-repeat", "Repeat", domain.CategoryGame)
	createDetailRunForTest(t, ctx, repo, "detail-run-repeat", "game")
	attemptedAt := time.Date(2026, 4, 24, 9, 0, 0, 0, time.UTC)
	detail := domain.WorkDetail{
		WorkHref:      work.Href,
		Category:      domain.CategoryGame,
		Title:         "Repeat",
		LastFetchedAt: attemptedAt,
	}

	if err := repo.SaveWorkDetail(ctx, detail, attemptedAt, "detail-run-repeat"); err != nil {
		t.Fatalf("first SaveWorkDetail() error = %v", err)
	}
	if err := repo.SaveWorkDetail(ctx, detail, attemptedAt, "detail-run-repeat"); err != nil {
		t.Fatalf("second SaveWorkDetail() error = %v", err)
	}

	snapshotCount, err := repo.CountWorkDetailSnapshotsByWorkHref(ctx, work.Href)
	if err != nil {
		t.Fatalf("CountWorkDetailSnapshotsByWorkHref() error = %v", err)
	}
	if snapshotCount != 1 {
		t.Fatalf("expected deduplicated snapshot count 1, got %d", snapshotCount)
	}
}

func TestRepositoryDetailFetchStateRunningAndFailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-state.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	work := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/movie/state", "State", domain.CategoryMovie)
	runningAt := time.Date(2026, 4, 24, 10, 0, 0, 0, time.UTC)
	createDetailRunForTest(t, ctx, repo, "detail-run-running", "movie")
	if err := repo.MarkDetailRunning(ctx, work.Href, runningAt, "detail-run-running"); err != nil {
		t.Fatalf("MarkDetailRunning() error = %v", err)
	}
	state, err := repo.GetDetailFetchState(ctx, work.Href)
	if err != nil {
		t.Fatalf("GetDetailFetchState(running) error = %v", err)
	}
	if state.Status != DetailFetchStatusRunning || !state.LastAttemptedAt.Valid || state.LastAttemptedAt.String != runningAt.Format(time.RFC3339) {
		t.Fatalf("unexpected running state: %+v", state)
	}
	if !state.LastRunID.Valid || state.LastRunID.String != "detail-run-running" {
		t.Fatalf("unexpected running run id: %+v", state)
	}

	failedAt := runningAt.Add(5 * time.Minute)
	createDetailRunForTest(t, ctx, repo, "detail-run-failed", "movie")
	if err := repo.MarkDetailFailed(ctx, work.Href, failedAt, "detail-run-failed", DetailFetchFailure{
		Message:    "boom",
		ErrorType:  "parse",
		ErrorStage: "parse",
	}); err != nil {
		t.Fatalf("MarkDetailFailed() error = %v", err)
	}
	state, err = repo.GetDetailFetchState(ctx, work.Href)
	if err != nil {
		t.Fatalf("GetDetailFetchState(failed) error = %v", err)
	}
	if state.Status != DetailFetchStatusFailed || !state.LastAttemptedAt.Valid || state.LastAttemptedAt.String != failedAt.Format(time.RFC3339) {
		t.Fatalf("unexpected failed state: %+v", state)
	}
	if state.LastFetchedAt.Valid {
		t.Fatalf("expected failed state to preserve empty last_fetched_at, got %+v", state)
	}
	if !state.LastError.Valid || state.LastError.String != "boom" {
		t.Fatalf("unexpected last_error: %+v", state)
	}
	if !state.LastErrorType.Valid || state.LastErrorType.String != "parse" || !state.LastErrorStage.Valid || state.LastErrorStage.String != "parse" {
		t.Fatalf("unexpected error classification: %+v", state)
	}
	if !state.LastRunID.Valid || state.LastRunID.String != "detail-run-failed" || state.UpdatedAt == "" {
		t.Fatalf("unexpected failed state metadata: %+v", state)
	}
}

func TestRepositoryListDetailCandidatesSkipsSucceededByDefault(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-candidates.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	succeeded := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/succeeded", "Succeeded", domain.CategoryGame)
	pending := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/pending", "Pending", domain.CategoryGame)
	createDetailRunForTest(t, ctx, repo, "detail-run-success", "game")
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      succeeded.Href,
		Category:      domain.CategoryGame,
		Title:         "Succeeded",
		LastFetchedAt: time.Now().UTC(),
	}, time.Now().UTC(), "detail-run-success"); err != nil {
		t.Fatalf("SaveWorkDetail() error = %v", err)
	}

	rows, err := repo.ListDetailCandidates(ctx, ListDetailCandidatesFilter{Category: string(domain.CategoryGame)})
	if err != nil {
		t.Fatalf("ListDetailCandidates() error = %v", err)
	}
	if len(rows) != 1 || rows[0].Work.Href != pending.Href {
		t.Fatalf("expected only pending candidate, got %+v", rows)
	}

	rows, err = repo.ListDetailCandidates(ctx, ListDetailCandidatesFilter{Category: string(domain.CategoryGame), Force: true})
	if err != nil {
		t.Fatalf("ListDetailCandidates(force) error = %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 force candidates, got %+v", rows)
	}
}

func TestRepositoryListDetailCandidatesMatchesNormalizedWorkHref(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-candidates-normalized.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	work := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/baldurs-gate-3/", "Baldur's Gate 3", domain.CategoryGame)

	rows, err := repo.ListDetailCandidates(ctx, ListDetailCandidatesFilter{
		WorkHref: "https://www.metacritic.com/game/baldurs-gate-3",
	})
	if err != nil {
		t.Fatalf("ListDetailCandidates() error = %v", err)
	}
	if len(rows) != 1 || rows[0].Work.Href != work.Href {
		t.Fatalf("expected normalized href match, got %+v", rows)
	}
}

func TestRepositoryRecoverStaleDetailFetchStatesOnlyRecoversStale(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-recovery.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	stale := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/stale", "Stale", domain.CategoryGame)
	fresh := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/fresh", "Fresh", domain.CategoryGame)
	other := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/movie/other", "Other", domain.CategoryMovie)

	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	createDetailRunForTest(t, ctx, repo, "old-run", "game")
	createDetailRunForTest(t, ctx, repo, "fresh-run", "game")
	createDetailRunForTest(t, ctx, repo, "other-run", "movie")
	createDetailRunForTest(t, ctx, repo, "recovery-run", "game")
	if err := repo.MarkDetailRunning(ctx, stale.Href, now.Add(-20*time.Minute), "old-run"); err != nil {
		t.Fatalf("MarkDetailRunning(stale) error = %v", err)
	}
	if err := repo.MarkDetailRunning(ctx, fresh.Href, now.Add(-5*time.Minute), "fresh-run"); err != nil {
		t.Fatalf("MarkDetailRunning(fresh) error = %v", err)
	}
	if err := repo.MarkDetailRunning(ctx, other.Href, now.Add(-20*time.Minute), "other-run"); err != nil {
		t.Fatalf("MarkDetailRunning(other) error = %v", err)
	}

	recovered, err := repo.RecoverStaleDetailFetchStates(ctx, ListDetailCandidatesFilter{
		Category: string(domain.CategoryGame),
	}, now.Add(-15*time.Minute), "recovery-run")
	if err != nil {
		t.Fatalf("RecoverStaleDetailFetchStates() error = %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered row, got %d", recovered)
	}

	staleState, err := repo.GetDetailFetchState(ctx, stale.Href)
	if err != nil {
		t.Fatalf("GetDetailFetchState(stale) error = %v", err)
	}
	if staleState.Status != DetailFetchStatusFailed || !staleState.LastErrorType.Valid || staleState.LastErrorType.String != "state_recovered" || !staleState.LastErrorStage.Valid || staleState.LastErrorStage.String != "recovery" {
		t.Fatalf("unexpected stale state after recovery: %+v", staleState)
	}
	if !staleState.LastRunID.Valid || staleState.LastRunID.String != "recovery-run" {
		t.Fatalf("unexpected recovered run id: %+v", staleState)
	}

	freshState, err := repo.GetDetailFetchState(ctx, fresh.Href)
	if err != nil {
		t.Fatalf("GetDetailFetchState(fresh) error = %v", err)
	}
	if freshState.Status != DetailFetchStatusRunning {
		t.Fatalf("expected fresh running state to remain running, got %+v", freshState)
	}

	otherState, err := repo.GetDetailFetchState(ctx, other.Href)
	if err != nil {
		t.Fatalf("GetDetailFetchState(other) error = %v", err)
	}
	if otherState.Status != DetailFetchStatusRunning {
		t.Fatalf("expected filtered-out category to remain untouched, got %+v", otherState)
	}
}

func TestInitSchemaBackfillsLegacyDetailFetchStateColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-detail-state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `
CREATE TABLE crawl_runs (
    run_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    task_name TEXT NOT NULL,
    category TEXT NOT NULL,
    metric TEXT NOT NULL,
    filter_key TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    error_message TEXT
);
CREATE TABLE works (
    href TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    image_url TEXT,
    release_date TEXT,
    category TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE detail_fetch_state (
    work_href TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    last_fetched_at TEXT,
    last_error TEXT,
    FOREIGN KEY(work_href) REFERENCES works(href)
);
INSERT INTO works(href, name, category) VALUES ('https://www.metacritic.com/game/legacy', 'Legacy', 'game');
INSERT INTO detail_fetch_state(work_href, status, last_fetched_at, last_error) VALUES ('https://www.metacritic.com/game/legacy', 'running', NULL, NULL);
`); err != nil {
		t.Fatalf("seed legacy schema error = %v", err)
	}

	if err := InitSchema(ctx, db, assets.SchemaSQL); err != nil {
		t.Fatalf("InitSchema() error = %v", err)
	}

	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_attempted_at")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_run_id")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_error_type")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "last_error_stage")
	assertHasColumn(t, ctx, db, "detail_fetch_state", "updated_at")

	repo := NewRepository(db)
	state, err := repo.GetDetailFetchState(ctx, "https://www.metacritic.com/game/legacy")
	if err != nil {
		t.Fatalf("GetDetailFetchState() error = %v", err)
	}
	if !state.LastAttemptedAt.Valid || state.UpdatedAt == "" {
		t.Fatalf("expected legacy running row to be backfilled, got %+v", state)
	}
}

func TestRepositoryListWorkDetailsFiltersCurrentView(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-read-list.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	gameWork := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/listed-game", "Listed Game", domain.CategoryGame)
	movieWork := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/movie/listed-movie", "Listed Movie", domain.CategoryMovie)
	createDetailRunForTest(t, ctx, repo, "detail-run-list", "all")

	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      gameWork.Href,
		Category:      domain.CategoryGame,
		Title:         "Listed Game",
		Metascore:     "96",
		LastFetchedAt: time.Date(2026, 4, 24, 8, 10, 0, 0, time.UTC),
	}, time.Date(2026, 4, 24, 8, 9, 0, 0, time.UTC), "detail-run-list"); err != nil {
		t.Fatalf("SaveWorkDetail(game) error = %v", err)
	}
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      movieWork.Href,
		Category:      domain.CategoryMovie,
		Title:         "Listed Movie",
		UserScore:     "8.1",
		LastFetchedAt: time.Date(2026, 4, 24, 8, 11, 0, 0, time.UTC),
	}, time.Date(2026, 4, 24, 8, 10, 0, 0, time.UTC), "detail-run-list"); err != nil {
		t.Fatalf("SaveWorkDetail(movie) error = %v", err)
	}

	rows, err := repo.ListWorkDetails(ctx, ListWorkDetailsFilter{
		Category: string(domain.CategoryGame),
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("ListWorkDetails(category) error = %v", err)
	}
	if len(rows) != 1 || rows[0].WorkHref != gameWork.Href {
		t.Fatalf("expected only game row, got %+v", rows)
	}

	rows, err = repo.ListWorkDetails(ctx, ListWorkDetailsFilter{
		WorkHref: movieWork.Href,
	})
	if err != nil {
		t.Fatalf("ListWorkDetails(workHref) error = %v", err)
	}
	if len(rows) != 1 || rows[0].WorkHref != movieWork.Href {
		t.Fatalf("expected only movie row, got %+v", rows)
	}
}

func TestRepositoryCompareWorkDetailsRecognizesChangeTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "details-compare.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := NewRepository(db)
	changed := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/changed", "Changed", domain.CategoryGame)
	unchanged := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/unchanged", "Unchanged", domain.CategoryGame)
	removed := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/removed", "Removed", domain.CategoryGame)
	added := seedDetailWork(t, ctx, repo, "https://www.metacritic.com/game/added", "Added", domain.CategoryGame)

	createDetailRunForTest(t, ctx, repo, "detail-run-a", "game")
	createDetailRunForTest(t, ctx, repo, "detail-run-b", "game")

	runAAttempted := time.Date(2026, 4, 24, 8, 0, 0, 0, time.UTC)
	runBAttempted := runAAttempted.Add(30 * time.Minute)
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      changed.Href,
		Category:      domain.CategoryGame,
		Title:         "Changed",
		Metascore:     "90",
		Tagline:       "old",
		LastFetchedAt: runAAttempted,
	}, runAAttempted, "detail-run-a"); err != nil {
		t.Fatalf("SaveWorkDetail(changed a) error = %v", err)
	}
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      changed.Href,
		Category:      domain.CategoryGame,
		Title:         "Changed",
		Metascore:     "95",
		Tagline:       "new",
		Details:       domain.WorkDetailExtras{Genres: []string{"RPG"}},
		LastFetchedAt: runBAttempted,
	}, runBAttempted, "detail-run-b"); err != nil {
		t.Fatalf("SaveWorkDetail(changed b) error = %v", err)
	}

	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      unchanged.Href,
		Category:      domain.CategoryGame,
		Title:         "Unchanged",
		Metascore:     "88",
		LastFetchedAt: runAAttempted,
	}, runAAttempted, "detail-run-a"); err != nil {
		t.Fatalf("SaveWorkDetail(unchanged a) error = %v", err)
	}
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      unchanged.Href,
		Category:      domain.CategoryGame,
		Title:         "Unchanged",
		Metascore:     "88",
		LastFetchedAt: runBAttempted,
	}, runBAttempted, "detail-run-b"); err != nil {
		t.Fatalf("SaveWorkDetail(unchanged b) error = %v", err)
	}

	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      removed.Href,
		Category:      domain.CategoryGame,
		Title:         "Removed",
		Metascore:     "82",
		LastFetchedAt: runAAttempted,
	}, runAAttempted, "detail-run-a"); err != nil {
		t.Fatalf("SaveWorkDetail(removed a) error = %v", err)
	}

	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:      added.Href,
		Category:      domain.CategoryGame,
		Title:         "Added",
		Metascore:     "99",
		LastFetchedAt: runBAttempted,
	}, runBAttempted, "detail-run-b"); err != nil {
		t.Fatalf("SaveWorkDetail(added b) error = %v", err)
	}

	rows, err := repo.CompareWorkDetails(ctx, CompareWorkDetailsFilter{
		FromRunID: "detail-run-a",
		ToRunID:   "detail-run-b",
		Category:  string(domain.CategoryGame),
	})
	if err != nil {
		t.Fatalf("CompareWorkDetails() error = %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 non-unchanged rows, got %+v", rows)
	}

	changeTypes := map[string]string{}
	for _, row := range rows {
		changeTypes[row.WorkHref] = row.ChangeType
	}
	if changeTypes[changed.Href] != "changed" {
		t.Fatalf("expected changed row, got %+v", rows)
	}
	if changeTypes[removed.Href] != "removed" {
		t.Fatalf("expected removed row, got %+v", rows)
	}
	if changeTypes[added.Href] != "added" {
		t.Fatalf("expected added row, got %+v", rows)
	}

	withUnchanged, err := repo.CompareWorkDetails(ctx, CompareWorkDetailsFilter{
		FromRunID:        "detail-run-a",
		ToRunID:          "detail-run-b",
		Category:         string(domain.CategoryGame),
		IncludeUnchanged: true,
	})
	if err != nil {
		t.Fatalf("CompareWorkDetails(include unchanged) error = %v", err)
	}
	if len(withUnchanged) != 4 {
		t.Fatalf("expected 4 rows with unchanged included, got %+v", withUnchanged)
	}

	filtered, err := repo.CompareWorkDetails(ctx, CompareWorkDetailsFilter{
		FromRunID: "detail-run-a",
		ToRunID:   "detail-run-b",
		WorkHref:  changed.Href,
	})
	if err != nil {
		t.Fatalf("CompareWorkDetails(work href) error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].WorkHref != changed.Href || filtered[0].ChangeType != "changed" {
		t.Fatalf("expected single changed row, got %+v", filtered)
	}

	filteredNoSlash, err := repo.CompareWorkDetails(ctx, CompareWorkDetailsFilter{
		FromRunID: "detail-run-a",
		ToRunID:   "detail-run-b",
		WorkHref:  strings.TrimSuffix(changed.Href, "/"),
	})
	if err != nil {
		t.Fatalf("CompareWorkDetails(normalized work href) error = %v", err)
	}
	if len(filteredNoSlash) != 1 || filteredNoSlash[0].WorkHref != changed.Href {
		t.Fatalf("expected normalized work href compare match, got %+v", filteredNoSlash)
	}
}

func seedDetailWork(t *testing.T, ctx context.Context, repo *Repository, href string, name string, category domain.Category) domain.Work {
	t.Helper()

	work := domain.Work{
		Name:     name,
		Href:     href,
		Category: category,
	}
	if err := repo.UpsertWork(ctx, work); err != nil {
		t.Fatalf("UpsertWork() error = %v", err)
	}
	return work
}

func assertHasColumn(t *testing.T, ctx context.Context, db *sql.DB, table string, column string) {
	t.Helper()

	ok, err := hasColumn(ctx, db, table, column)
	if err != nil {
		t.Fatalf("hasColumn(%s.%s) error = %v", table, column, err)
	}
	if !ok {
		t.Fatalf("expected %s.%s to exist", table, column)
	}
}

func assertHasTable(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()

	ok, err := hasTable(ctx, db, table)
	if err != nil {
		t.Fatalf("hasTable(%s) error = %v", table, err)
	}
	if !ok {
		t.Fatalf("expected table %s to exist", table)
	}
}

func createDetailRunForTest(t *testing.T, ctx context.Context, repo *Repository, runID string, category string) {
	t.Helper()

	if err := repo.CreateDetailCrawlRun(ctx, runID, category, "detail-"+category, "href=all|force=0|limit=all", time.Date(2026, 4, 24, 8, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("CreateDetailCrawlRun(%s) error = %v", runID, err)
	}
}
