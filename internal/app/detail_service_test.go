package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GoFurry/metacritic-harvester/internal/config"
	"github.com/GoFurry/metacritic-harvester/internal/domain"
	"github.com/GoFurry/metacritic-harvester/internal/storage"
)

func TestDetailServiceRunSuccessCreatesCompletedCrawlRun(t *testing.T) {
	t.Parallel()

	requests := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests[r.URL.Path]++
		_, _ = w.Write([]byte(detailServiceGameHTML("Game " + r.URL.Path)))
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-service-success.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Alpha", Href: server.URL + "/game/alpha", Category: domain.CategoryGame},
		{Name: "Beta", Href: server.URL + "/game/beta", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.now = func() time.Time { return time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC) }
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.RunID == "" || result.Total != 2 || result.Processed != 2 || result.Fetched != 2 || result.Failed != 0 || result.Failures != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if requests["/game/alpha"] != 1 || requests["/game/beta"] != 1 {
		t.Fatalf("unexpected requests: %+v", requests)
	}

	db, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer db.Close()
	repo := storage.NewRepository(db)
	run, err := repo.GetCrawlRun(ctx, result.RunID)
	if err != nil {
		t.Fatalf("GetCrawlRun() error = %v", err)
	}
	if run.Source != "crawl detail" || run.Metric != "detail" || run.Category != "game" || run.TaskName != "detail-game" || run.Status != "completed" {
		t.Fatalf("unexpected crawl run: %+v", run)
	}
	snapshotCount, err := repo.CountWorkDetailSnapshots(ctx)
	if err != nil {
		t.Fatalf("CountWorkDetailSnapshots() error = %v", err)
	}
	if snapshotCount != 2 {
		t.Fatalf("expected 2 snapshots, got %d", snapshotCount)
	}
}

func TestDetailServiceRunPersistsNuxtWhereToBuy(t *testing.T) {
	t.Parallel()

	nuxtRaw := readRootNuxtSample(t, "metacritic-nuxt-data-game-sample.txt")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(detailServiceGameHTMLWithNuxt("Game With Buy Options", nuxtRaw)))
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-service-nuxt.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Nuxt Game", Href: server.URL + "/game/nuxt", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Fetched != 1 || result.Failed != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}

	db, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer db.Close()
	repo := storage.NewRepository(db)
	row, rowErr := repo.GetWorkDetail(ctx, server.URL+"/game/nuxt")
	if rowErr != nil {
		t.Fatalf("GetWorkDetail() error = %v", rowErr)
	}
	if !strings.Contains(row.DetailsJson, "\"where_to_buy\"") || !strings.Contains(row.DetailsJson, "Amazon") {
		t.Fatalf("expected details_json to include where_to_buy payload, got %q", row.DetailsJson)
	}
}

func TestDetailServiceRunFailureMarksCrawlRunFailed(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "bad") {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(detailServiceGameHTML("Good Game")))
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-service-failure.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Bad Game", Href: server.URL + "/game/bad", Category: domain.CategoryGame},
		{Name: "Good Game", Href: server.URL + "/game/good", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.now = func() time.Time { return time.Date(2026, 4, 24, 13, 0, 0, 0, time.UTC) }
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err == nil {
		t.Fatal("expected Run() error")
	}
	if result.RunID == "" || result.Total != 2 || result.Processed != 2 || result.Fetched != 1 || result.Failed != 1 || result.Failures != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	db, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer db.Close()
	repo := storage.NewRepository(db)
	run, runErr := repo.GetCrawlRun(ctx, result.RunID)
	if runErr != nil {
		t.Fatalf("GetCrawlRun() error = %v", runErr)
	}
	if run.Status != "failed" || !run.ErrorMessage.Valid {
		t.Fatalf("expected failed crawl run, got %+v", run)
	}

	state, stateErr := repo.GetDetailFetchState(ctx, server.URL+"/game/bad")
	if stateErr != nil {
		t.Fatalf("GetDetailFetchState() error = %v", stateErr)
	}
	if state.Status != storage.DetailFetchStatusFailed || !state.LastErrorType.Valid || state.LastErrorType.String != detailErrorTypeHTTP5xx || !state.LastErrorStage.Valid || state.LastErrorStage.String != detailErrorStageRequest {
		t.Fatalf("expected failed state with http_5xx/request, got %+v", state)
	}
}

func TestDetailServiceRunRecoversStaleAndSkipsFreshRunning(t *testing.T) {
	t.Parallel()

	requests := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests[r.URL.Path]++
		_, _ = w.Write([]byte(detailServiceGameHTML("Recovered " + r.URL.Path)))
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-service-recovery.db")
	repo := seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Stale", Href: server.URL + "/game/stale", Category: domain.CategoryGame},
		{Name: "Fresh", Href: server.URL + "/game/fresh", Category: domain.CategoryGame},
		{Name: "Pending", Href: server.URL + "/game/pending", Category: domain.CategoryGame},
	})

	baseNow := time.Date(2026, 4, 24, 14, 0, 0, 0, time.UTC)
	createDetailRunForServiceTest(t, ctx, repo, "old-run", "game")
	createDetailRunForServiceTest(t, ctx, repo, "fresh-run", "game")
	if err := repo.MarkDetailRunning(ctx, server.URL+"/game/stale", baseNow.Add(-20*time.Minute), "old-run"); err != nil {
		t.Fatalf("MarkDetailRunning(stale) error = %v", err)
	}
	if err := repo.MarkDetailRunning(ctx, server.URL+"/game/fresh", baseNow.Add(-5*time.Minute), "fresh-run"); err != nil {
		t.Fatalf("MarkDetailRunning(fresh) error = %v", err)
	}

	service := NewDetailService(DetailServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.now = func() time.Time { return baseNow }
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.RecoveredRunning != 1 || result.Total != 3 || result.Processed != 3 || result.Fetched != 2 || result.Skipped != 1 || result.Failed != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if requests["/game/fresh"] != 0 || requests["/game/stale"] != 1 || requests["/game/pending"] != 1 {
		t.Fatalf("unexpected requests after recovery: %+v", requests)
	}
}

func TestDetailServiceRequestClassificationAndRetryPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		path           string
		statuses       []int
		wantRequests   int
		wantType       string
		wantStage      string
		wantSuccess    bool
		wantSleepCalls []time.Duration
	}{
		{
			name:         "403 does not retry",
			path:         "/game/forbidden",
			statuses:     []int{http.StatusForbidden},
			wantRequests: 1,
			wantType:     detailErrorTypeHTTP403,
			wantStage:    detailErrorStageRequest,
		},
		{
			name:         "404 does not retry",
			path:         "/game/missing",
			statuses:     []int{http.StatusNotFound},
			wantRequests: 1,
			wantType:     detailErrorTypeHTTP404,
			wantStage:    detailErrorStageRequest,
		},
		{
			name:           "429 retries with long backoff",
			path:           "/game/rate-limited",
			statuses:       []int{http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusOK},
			wantRequests:   3,
			wantSuccess:    true,
			wantSleepCalls: []time.Duration{3 * time.Second, 6 * time.Second},
		},
		{
			name:           "500 retries with short backoff",
			path:           "/game/server-error",
			statuses:       []int{http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK},
			wantRequests:   3,
			wantSuccess:    true,
			wantSleepCalls: []time.Duration{1 * time.Second, 2 * time.Second},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				status := tt.statuses[requests-1]
				if status != http.StatusOK {
					http.Error(w, http.StatusText(status), status)
					return
				}
				_, _ = w.Write([]byte(detailServiceGameHTML("Recovered")))
			}))
			defer server.Close()

			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), strings.TrimPrefix(strings.ReplaceAll(tt.path, "/", "_"), "_")+".db")
			seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
				{Name: "Game", Href: server.URL + tt.path, Category: domain.CategoryGame},
			})

			service := NewDetailService(DetailServiceConfig{
				BaseURL:    server.URL,
				DBPath:     dbPath,
				MaxRetries: 2,
			})
			var sleeps []time.Duration
			service.sleep = func(d time.Duration) { sleeps = append(sleeps, d) }

			result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
			if tt.wantSuccess {
				if err != nil {
					t.Fatalf("Run() error = %v", err)
				}
				if result.Fetched != 1 || result.Failed != 0 {
					t.Fatalf("unexpected result: %+v", result)
				}
				if len(sleeps) != len(tt.wantSleepCalls) {
					t.Fatalf("unexpected sleep count: got %v want %v", sleeps, tt.wantSleepCalls)
				}
				for i := range sleeps {
					if sleeps[i] != tt.wantSleepCalls[i] {
						t.Fatalf("unexpected sleep sequence: got %v want %v", sleeps, tt.wantSleepCalls)
					}
				}
			} else {
				if err == nil {
					t.Fatal("expected Run() error")
				}
				if result.Failed != 1 || result.Failures != 1 {
					t.Fatalf("unexpected result: %+v", result)
				}

				db, openErr := storage.Open(ctx, dbPath)
				if openErr != nil {
					t.Fatalf("Open() error = %v", openErr)
				}
				repo := storage.NewRepository(db)
				state, stateErr := repo.GetDetailFetchState(ctx, server.URL+tt.path)
				_ = db.Close()
				if stateErr != nil {
					t.Fatalf("GetDetailFetchState() error = %v", stateErr)
				}
				if !state.LastErrorType.Valid || state.LastErrorType.String != tt.wantType || !state.LastErrorStage.Valid || state.LastErrorStage.String != tt.wantStage {
					t.Fatalf("unexpected classification: %+v", state)
				}
			}

			if requests != tt.wantRequests {
				t.Fatalf("unexpected request count: got %d want %d", requests, tt.wantRequests)
			}
		})
	}
}

func TestDetailServiceParseFailureDoesNotRetry(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(`<html><body><div>missing title</div></body></html>`))
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-service-parse.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Broken Game", Href: server.URL + "/game/broken", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 3,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err == nil {
		t.Fatal("expected Run() error")
	}
	if requests != 1 {
		t.Fatalf("expected parse failure to avoid retries, got %d requests", requests)
	}
	if result.Failed != 1 || result.Failures != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	db, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer db.Close()
	repo := storage.NewRepository(db)
	state, stateErr := repo.GetDetailFetchState(ctx, server.URL+"/game/broken")
	if stateErr != nil {
		t.Fatalf("GetDetailFetchState() error = %v", stateErr)
	}
	if !state.LastErrorType.Valid || state.LastErrorType.String != detailErrorTypeParse || !state.LastErrorStage.Valid || state.LastErrorStage.String != detailErrorStageParse {
		t.Fatalf("expected parse classification, got %+v", state)
	}
}

func TestDetailServiceRunWritesSnapshotsAcrossRuns(t *testing.T) {
	t.Parallel()

	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(detailServiceGameHTML("Game Snapshot")))
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-service-snapshots.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Snapshot", Href: server.URL + "/game/snapshot", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.sleep = func(time.Duration) {}

	if _, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame}); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if _, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame, Force: true}); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	db, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer db.Close()
	repo := storage.NewRepository(db)
	snapshotCount, err := repo.CountWorkDetailSnapshotsByWorkHref(ctx, server.URL+"/game/snapshot")
	if err != nil {
		t.Fatalf("CountWorkDetailSnapshotsByWorkHref() error = %v", err)
	}
	if snapshotCount != 2 {
		t.Fatalf("expected 2 snapshot rows after two successful runs, got %d", snapshotCount)
	}
	if requests != 2 {
		t.Fatalf("expected 2 requests across runs, got %d", requests)
	}
}

func TestDetailServiceRunWithConcurrency(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Millisecond)
		_, _ = w.Write([]byte(detailServiceGameHTML("Concurrent " + r.URL.Path)))
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-service-concurrency.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "One", Href: server.URL + "/game/one", Category: domain.CategoryGame},
		{Name: "Two", Href: server.URL + "/game/two", Category: domain.CategoryGame},
		{Name: "Three", Href: server.URL + "/game/three", Category: domain.CategoryGame},
		{Name: "Four", Href: server.URL + "/game/four", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame, Concurrency: 3})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Total != 4 || result.Processed != 4 || result.Fetched != 4 || result.Failures != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}

	db, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer db.Close()
	repo := storage.NewRepository(db)
	snapshotCount, err := repo.CountWorkDetailSnapshots(ctx)
	if err != nil {
		t.Fatalf("CountWorkDetailSnapshots() error = %v", err)
	}
	if snapshotCount != 4 {
		t.Fatalf("expected 4 snapshots, got %d", snapshotCount)
	}
}

func TestDetailServiceRunWithAPISourceAndEnrich(t *testing.T) {
	t.Parallel()

	nuxtRaw := readRootNuxtSample(t, "metacritic-nuxt-data-game-sample.txt")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/composer/metacritic/pages/games/alpha/web":
			_, _ = w.Write([]byte(`{
				"components": [
					{
						"meta": {"componentName": "product"},
						"data": {
							"item": {
								"title": "Alpha",
								"description": "Summary text.",
								"releaseDate": "2026-04-23",
								"criticScoreSummary": {"score": "91", "sentiment": "Universal Acclaim", "reviewCount": 12},
								"userScore": {"score": "8.4", "sentiment": "Generally Favorable", "reviewCount": 34},
								"rating": "M",
								"genres": [{"name": "Action RPG"}],
								"platform": "PC",
								"platforms": [{"name": "PC", "criticScoreSummary": {"url": "/game/alpha/critic-reviews?platform=pc", "score": "91", "reviewCount": 12}}],
								"production": {
									"companies": [
										{"typeName": "developer", "name": "Studio A"},
										{"typeName": "publisher", "name": "Publisher A"}
									]
								}
							}
						}
					}
				]
			}`))
		case "/game/alpha":
			_, _ = w.Write([]byte(detailServiceGameHTMLWithNuxt("Alpha", nuxtRaw)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-api.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Alpha", Href: server.URL + "/game/alpha", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:        server.URL,
		BackendBaseURL: server.URL,
		Source:         config.CrawlSourceAPI,
		DBPath:         dbPath,
		MaxRetries:     0,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Fetched != 1 || result.Failures != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}

	db, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer db.Close()
	repo := storage.NewRepository(db)
	row, rowErr := repo.GetWorkDetail(ctx, server.URL+"/game/alpha")
	if rowErr != nil {
		t.Fatalf("GetWorkDetail() error = %v", rowErr)
	}
	if !row.Metascore.Valid || row.Metascore.String != "91" || !strings.Contains(row.DetailsJson, "\"where_to_buy\"") {
		t.Fatalf("expected api detail + enrich payload, got %+v", row)
	}
}

func TestDetailServiceRunAutoFallsBackToHTML(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/composer/metacritic/pages/games/fallback/web":
			http.Error(w, "boom", http.StatusInternalServerError)
		case "/game/fallback":
			_, _ = w.Write([]byte(detailServiceGameHTML("Fallback Game")))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-auto.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Fallback", Href: server.URL + "/game/fallback", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:        server.URL,
		BackendBaseURL: server.URL,
		Source:         config.CrawlSourceAuto,
		DBPath:         dbPath,
		MaxRetries:     0,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Fetched != 1 || result.Failures != 0 {
		t.Fatalf("unexpected auto fallback result: %+v", result)
	}
}

func TestDetailServiceAPISourceIgnoresEnrichFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/composer/metacritic/pages/games/no-enrich/web":
			_, _ = w.Write([]byte(`{
				"components": [
					{
						"meta": {"componentName": "product"},
						"data": {"item": {"title": "No Enrich", "description": "Summary", "releaseDate": "2026-04-23"}}
					}
				]
			}`))
		case "/game/no-enrich":
			http.Error(w, "no html", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "detail-api-enrich-failure.db")
	seedDetailServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "No Enrich", Href: server.URL + "/game/no-enrich", Category: domain.CategoryGame},
	})

	service := NewDetailService(DetailServiceConfig{
		BaseURL:        server.URL,
		BackendBaseURL: server.URL,
		Source:         config.CrawlSourceAPI,
		DBPath:         dbPath,
		MaxRetries:     0,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.DetailTask{Category: domain.CategoryGame})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.Fetched != 1 || result.Failures != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func seedDetailServiceDB(t *testing.T, ctx context.Context, dbPath string, works []domain.Work) *storage.Repository {
	t.Helper()

	db, err := storage.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	repo := storage.NewRepository(db)
	for _, work := range works {
		if err := repo.UpsertWork(ctx, work); err != nil {
			_ = db.Close()
			t.Fatalf("UpsertWork() error = %v", err)
		}
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return repo
}

func detailServiceGameHTML(title string) string {
	return `<html><body>
<h1 class="hero-title__text">` + title + `</h1>
<div class="product-hero__release-date"><div class="hero-release-date__value">Apr 23, 2026</div></div>
<div class="c-game-details__summary-description">Summary text.</div>
</body></html>`
}

func detailServiceGameHTMLWithNuxt(title string, nuxtRaw string) string {
	return `<html><body>
<h1 class="hero-title__text">` + title + `</h1>
<div class="product-hero__release-date"><div class="hero-release-date__value">Apr 23, 2026</div></div>
<div class="c-game-details__summary-description">Summary text.</div>
<script id="__NUXT_DATA__">` + nuxtRaw + `</script>
</body></html>`
}

func readRootNuxtSample(t *testing.T, name string) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}

	body, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "docs", "sample", name))
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", name, err)
	}
	raw := strings.TrimSpace(string(body))
	if idx := strings.Index(raw, "</script>"); idx >= 0 {
		raw = raw[:idx]
	}
	if start := strings.Index(raw, ">"); strings.HasPrefix(raw, "<script") && start >= 0 {
		raw = raw[start+1:]
	}
	return strings.TrimSpace(raw)
}

func createDetailRunForServiceTest(t *testing.T, ctx context.Context, repo *storage.Repository, runID string, category string) {
	t.Helper()

	if err := repo.CreateDetailCrawlRun(ctx, runID, category, "detail-"+category, "href=all|force=0|limit=all", time.Date(2026, 4, 24, 8, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("CreateDetailCrawlRun(%s) error = %v", runID, err)
	}
}
