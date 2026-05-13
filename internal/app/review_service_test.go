package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

func TestReviewServiceRunPlansGamePlatformsAndMixedTypes(t *testing.T) {
	t.Parallel()

	server, requests := newReviewAPITestServer(t, reviewAPITestServerConfig{})
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "review-mixed.db")
	repo := seedReviewServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Baldur's Gate 3", Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame},
	})

	service := NewReviewService(ReviewServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.now = func() time.Time { return time.Date(2026, 4, 25, 10, 0, 0, 0, time.UTC) }
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryGame,
		ReviewType:  domain.ReviewTypeAll,
		Sentiment:   domain.ReviewSentimentPositive,
		Sort:        domain.ReviewSortScore,
		Concurrency: 2,
		PageSize:    20,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ScopesScheduled != 4 || result.ScopesFetched != 4 || result.ScopesFailed != 0 || result.ReviewsFetched != 6 || result.Failures != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.RequestedSource != string(config.CrawlSourceAPI) || result.EffectiveSource != string(config.CrawlSourceAPI) || result.FallbackUsed {
		t.Fatalf("expected reviews to report fixed api source, got %+v", result)
	}

	latestCount, err := repo.CountLatestReviews(ctx)
	if err != nil {
		t.Fatalf("CountLatestReviews() error = %v", err)
	}
	snapshotCount, err := repo.CountReviewSnapshots(ctx)
	if err != nil {
		t.Fatalf("CountReviewSnapshots() error = %v", err)
	}
	if latestCount != 6 || snapshotCount != 6 {
		t.Fatalf("unexpected review counts latest=%d snapshots=%d", latestCount, snapshotCount)
	}

	criticRequest := requests["/reviews/metacritic/critic/games/baldurs-gate-3/web?filterBySentiment=positive&limit=20&offset=0&platform=pc&sort=score"]
	if criticRequest != 1 {
		t.Fatalf("expected filtered critic request, got %+v", requests)
	}
}

func TestReviewServiceRunSkipsSucceededScopeUnlessForced(t *testing.T) {
	t.Parallel()

	server, requests := newReviewAPITestServer(t, reviewAPITestServerConfig{})
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "review-force.db")
	seedReviewServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "PK", Href: "https://www.metacritic.com/movie/pk/", Category: domain.CategoryMovie},
	})

	service := NewReviewService(ReviewServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.now = func() time.Time { return time.Date(2026, 4, 25, 11, 0, 0, 0, time.UTC) }
	service.sleep = func(time.Duration) {}

	first, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryMovie,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
	})
	if err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	second, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryMovie,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
	})
	if err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	third, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryMovie,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
		Force:       true,
	})
	if err != nil {
		t.Fatalf("third Run() error = %v", err)
	}

	if first.ScopesFetched != 1 || second.ScopesSkipped != 1 || third.ScopesFetched != 1 {
		t.Fatalf("unexpected force/skip results: first=%+v second=%+v third=%+v", first, second, third)
	}

	listPath := "/reviews/metacritic/critic/movies/pk/web?limit=20&offset=0"
	if requests[listPath] != 2 {
		t.Fatalf("expected list endpoint twice (initial + force), got %+v", requests)
	}
}

func TestReviewServiceRunTreatsEmptyListAsSucceededScope(t *testing.T) {
	t.Parallel()

	server, _ := newReviewAPITestServer(t, reviewAPITestServerConfig{
		ListFixtureByPath: map[string]string{
			"/reviews/metacritic/critic/movies/pk/web": "list_empty.json",
		},
	})
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "review-empty.db")
	repo := seedReviewServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "PK", Href: "https://www.metacritic.com/movie/pk/", Category: domain.CategoryMovie},
	})

	service := NewReviewService(ReviewServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryMovie,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ScopesFetched != 1 || result.ReviewsFetched != 0 || result.Failures != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}

	state, err := repo.GetReviewFetchState(ctx, domain.ReviewScope{
		WorkHref:   "https://www.metacritic.com/movie/pk/",
		Category:   domain.CategoryMovie,
		ReviewType: domain.ReviewTypeCritic,
	})
	if err != nil {
		t.Fatalf("GetReviewFetchState() error = %v", err)
	}
	if state.Status != storage.ReviewFetchStatusSucceeded {
		t.Fatalf("expected succeeded state, got %+v", state)
	}
}

func TestReviewServiceRunFailureAndScopeRerunRemainConsistent(t *testing.T) {
	t.Parallel()

	server, _ := newReviewAPITestServer(t, reviewAPITestServerConfig{
		StatusByPath: map[string][]int{
			"/reviews/metacritic/critic/movies/pk/web": {http.StatusInternalServerError},
		},
	})
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "review-rerun.db")
	repo := seedReviewServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "PK", Href: "https://www.metacritic.com/movie/pk/", Category: domain.CategoryMovie},
	})

	service := NewReviewService(ReviewServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	service.sleep = func(time.Duration) {}

	first, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryMovie,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
	})
	if err == nil {
		t.Fatal("expected first Run() to fail")
	}
	if first.ScopesFailed != 1 || first.Failures != 1 {
		t.Fatalf("unexpected failure result: %+v", first)
	}

	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reviewAPITestHandler(t, reviewAPITestServerConfig{}, nil)(w, r)
	})
	second, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryMovie,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
		Force:       true,
	})
	if err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if second.ScopesFetched != 1 || second.Failures != 0 {
		t.Fatalf("unexpected rerun result: %+v", second)
	}

	latestCount, err := repo.CountLatestReviews(ctx)
	if err != nil {
		t.Fatalf("CountLatestReviews() error = %v", err)
	}
	snapshotCount, err := repo.CountReviewSnapshots(ctx)
	if err != nil {
		t.Fatalf("CountReviewSnapshots() error = %v", err)
	}
	if latestCount != 2 || snapshotCount != 2 {
		t.Fatalf("expected stable rerun counts latest=2 snapshots=2, got latest=%d snapshots=%d", latestCount, snapshotCount)
	}
}

func TestReviewServiceRunContinueOnErrorCompletesRun(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/composer/metacritic/pages/movies-critic-reviews/pk/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "composer_game_critic.json")))
		case "/composer/metacritic/pages/movies-critic-reviews/bad/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "composer_game_critic.json")))
		case "/reviews/metacritic/critic/movies/pk/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "list_critic.json")))
		case "/reviews/metacritic/critic/movies/bad/web":
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "review-continue.db")
	repo := seedReviewServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "PK", Href: "https://www.metacritic.com/movie/pk/", Category: domain.CategoryMovie},
		{Name: "Bad", Href: "https://www.metacritic.com/movie/bad/", Category: domain.CategoryMovie},
	})

	service := NewReviewService(ReviewServiceConfig{
		BaseURL:         server.URL,
		DBPath:          dbPath,
		ContinueOnError: true,
		MaxRetries:      0,
	})
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryMovie,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
	})
	if err != nil {
		t.Fatalf("expected continue-on-error run to succeed, got %v", err)
	}
	if result.ScopesScheduled != 2 || result.ScopesFetched != 1 || result.ScopesFailed != 1 || result.Failures != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	latestCount, countErr := repo.CountLatestReviews(ctx)
	if countErr != nil {
		t.Fatalf("CountLatestReviews() error = %v", countErr)
	}
	if latestCount == 0 {
		t.Fatal("expected successful scope writes to remain persisted")
	}

	runDB, openErr := storage.Open(ctx, dbPath)
	if openErr != nil {
		t.Fatalf("Open() error = %v", openErr)
	}
	defer runDB.Close()
	runRepo := storage.NewRepository(runDB)
	run, runErr := runRepo.GetCrawlRun(ctx, result.RunID)
	if runErr != nil {
		t.Fatalf("GetCrawlRun() error = %v", runErr)
	}
	if run.Status != "completed" {
		t.Fatalf("expected completed crawl run, got %+v", run)
	}
}

func TestReviewServiceRunRecoversStaleRunningAndSkipsFreshRunning(t *testing.T) {
	t.Parallel()

	server, requests := newReviewAPITestServer(t, reviewAPITestServerConfig{})
	defer server.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "review-recovery.db")
	repo := seedReviewServiceDB(t, ctx, dbPath, []domain.Work{
		{Name: "Baldur's Gate 3", Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame},
	})

	oldAttempt := time.Date(2026, 4, 25, 8, 0, 0, 0, time.UTC)
	createReviewRunForTest(t, ctx, repo, "old-run", "game")
	createReviewRunForTest(t, ctx, repo, "fresh-run", "game")
	if err := repo.MarkReviewRunning(ctx, domain.ReviewScope{
		WorkHref:    "https://www.metacritic.com/game/baldurs-gate-3/",
		Category:    domain.CategoryGame,
		ReviewType:  domain.ReviewTypeCritic,
		PlatformKey: "pc",
	}, oldAttempt, "old-run"); err != nil {
		t.Fatalf("MarkReviewRunning(old) error = %v", err)
	}
	if err := repo.MarkReviewRunning(ctx, domain.ReviewScope{
		WorkHref:    "https://www.metacritic.com/game/baldurs-gate-3/",
		Category:    domain.CategoryGame,
		ReviewType:  domain.ReviewTypeCritic,
		PlatformKey: "playstation-5",
	}, oldAttempt.Add(14*time.Minute), "fresh-run"); err != nil {
		t.Fatalf("MarkReviewRunning(fresh) error = %v", err)
	}

	service := NewReviewService(ReviewServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})
	now := oldAttempt.Add(20 * time.Minute)
	service.now = func() time.Time { return now }
	service.sleep = func(time.Duration) {}

	result, err := service.Run(ctx, domain.ReviewTask{
		Category:    domain.CategoryGame,
		ReviewType:  domain.ReviewTypeCritic,
		Sentiment:   domain.ReviewSentimentAll,
		Concurrency: 1,
		PageSize:    20,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ScopesFetched != 1 || result.ScopesSkipped != 1 || result.Failures != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if requests["/reviews/metacritic/critic/games/baldurs-gate-3/web?limit=20&offset=0&platform=pc"] != 1 {
		t.Fatalf("expected stale scope to be retried, got %+v", requests)
	}
	if requests["/reviews/metacritic/critic/games/baldurs-gate-3/web?limit=20&offset=0&platform=playstation-5"] != 0 {
		t.Fatalf("expected fresh scope to be skipped, got %+v", requests)
	}

	staleState, err := repo.GetReviewFetchState(ctx, domain.ReviewScope{
		WorkHref:    "https://www.metacritic.com/game/baldurs-gate-3/",
		Category:    domain.CategoryGame,
		ReviewType:  domain.ReviewTypeCritic,
		PlatformKey: "pc",
	})
	if err != nil {
		t.Fatalf("GetReviewFetchState(stale) error = %v", err)
	}
	if staleState.Status != storage.ReviewFetchStatusSucceeded {
		t.Fatalf("expected stale state to recover and succeed, got %+v", staleState)
	}
}

type reviewAPITestServerConfig struct {
	ListFixtureByPath map[string]string
	StatusByPath      map[string][]int
}

func newReviewAPITestServer(t *testing.T, cfg reviewAPITestServerConfig) (*httptest.Server, map[string]int) {
	t.Helper()
	requests := make(map[string]int)
	return httptest.NewServer(reviewAPITestHandler(t, cfg, requests)), requests
}

func reviewAPITestHandler(t *testing.T, cfg reviewAPITestServerConfig, requests map[string]int) http.HandlerFunc {
	t.Helper()
	statusIndex := make(map[string]int)

	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		if r.URL.RawQuery != "" {
			key += "?" + r.URL.RawQuery
		}
		if requests != nil {
			requests[key]++
		}

		if statuses := cfg.StatusByPath[r.URL.Path]; len(statuses) > 0 {
			idx := statusIndex[r.URL.Path]
			if idx >= len(statuses) {
				idx = len(statuses) - 1
			}
			statusIndex[r.URL.Path]++
			w.WriteHeader(statuses[idx])
			_, _ = w.Write([]byte(`{"error":"boom"}`))
			return
		}

		if fixture, ok := cfg.ListFixtureByPath[r.URL.Path]; ok {
			_, _ = w.Write([]byte(readReviewServiceFixture(t, fixture)))
			return
		}

		switch r.URL.Path {
		case "/composer/metacritic/pages/games-critic-reviews/baldurs-gate-3/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "composer_game_critic.json")))
		case "/composer/metacritic/pages/movies-critic-reviews/pk/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "composer_game_critic.json")))
		case "/reviews/metacritic/critic/games/baldurs-gate-3/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "list_critic.json")))
		case "/reviews/metacritic/user/games/baldurs-gate-3/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "list_user.json")))
		case "/reviews/metacritic/critic/movies/pk/web":
			_, _ = w.Write([]byte(readReviewServiceFixture(t, "list_critic.json")))
		default:
			http.NotFound(w, r)
		}
	}
}

func seedReviewServiceDB(t *testing.T, ctx context.Context, dbPath string, works []domain.Work) *storage.Repository {
	t.Helper()

	db, err := storage.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	repo := storage.NewRepository(db)
	for _, work := range works {
		if err := repo.UpsertWork(ctx, work); err != nil {
			t.Fatalf("UpsertWork(%s) error = %v", work.Href, err)
		}
	}
	return repo
}

func createReviewRunForTest(t *testing.T, ctx context.Context, repo *storage.Repository, runID string, category string) {
	t.Helper()
	if err := repo.CreateReviewCrawlRun(ctx, runID, "crawl reviews", "reviews-test", category, "test=1", time.Date(2026, 4, 25, 7, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("CreateReviewCrawlRun(%s) error = %v", runID, err)
	}
}

func readReviewServiceFixture(t *testing.T, name string) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "source", "metacritic", "api", "testdata", "reviews", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return string(content)
}

func TestReviewServiceRuntimePolicyUsesTaskConcurrency(t *testing.T) {
	t.Parallel()

	service := NewReviewService(ReviewServiceConfig{})
	policy := service.runtimePolicy(3)
	if policy.MaxInFlight != 3 {
		t.Fatalf("expected max in-flight to follow task concurrency, got %+v", policy)
	}
	if policy.Timeout <= 0 {
		t.Fatalf("expected positive timeout, got %+v", policy)
	}
}
