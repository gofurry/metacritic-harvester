package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GoFurry/metacritic-harvester/internal/config"
	"github.com/GoFurry/metacritic-harvester/internal/domain"
	"github.com/GoFurry/metacritic-harvester/internal/storage"
)

func TestListServiceRun(t *testing.T) {
	t.Parallel()

	requests := make(chan string, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.String()
		switch r.URL.Path {
		case "/browse/game/":
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(pageTwoHTML))
				return
			}
			_, _ = w.Write([]byte(pageOneHTML))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "integration.db")
	service := NewListService(ListServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 1,
	})

	result, err := service.Run(context.Background(), domain.ListTask{
		Category: "game",
		Metric:   "metascore",
		MaxPages: 2,
		Filter: domain.Filter{
			ReleaseYearMin: intPtr(2011),
			ReleaseYearMax: intPtr(2014),
			Platforms:      []string{"pc", "ps5"},
			Genres:         []string{"action", "rpg"},
			ReleaseTypes:   []string{"coming-soon"},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.PagesVisited != 2 {
		t.Fatalf("expected 2 pages visited, got %d", result.PagesVisited)
	}
	if result.PagesScheduled != 2 || result.PagesSucceeded != 2 || result.PagesWritten != 2 {
		t.Fatalf("expected page stats 2/2/2, got scheduled=%d succeeded=%d written=%d", result.PagesScheduled, result.PagesSucceeded, result.PagesWritten)
	}
	if result.RunID == "" {
		t.Fatal("expected run id to be set")
	}
	if result.WorksUpserted != 2 {
		t.Fatalf("expected 2 works upserted, got %d", result.WorksUpserted)
	}
	if result.ListEntriesInserted != 2 {
		t.Fatalf("expected 2 list entries inserted, got %d", result.ListEntriesInserted)
	}
	if result.LatestEntriesUpserted != 2 {
		t.Fatalf("expected 2 latest list entries upserted, got %d", result.LatestEntriesUpserted)
	}
	if result.Failures != 0 {
		t.Fatalf("expected 0 failures, got %d", result.Failures)
	}

	close(requests)
	var visited []string
	for req := range requests {
		visited = append(visited, req)
	}
	if len(visited) == 0 {
		t.Fatal("expected at least one request")
	}
	if !strings.Contains(visited[0], "releaseYearMin=2011") ||
		!strings.Contains(visited[0], "releaseYearMax=2014") ||
		!strings.Contains(visited[0], "platform=pc") ||
		!strings.Contains(visited[0], "genre=action") ||
		!strings.Contains(visited[0], "releaseType=coming-soon") {
		t.Fatalf("expected filters in first request, got %q", visited[0])
	}
	if len(visited) > 1 && !strings.Contains(visited[1], "page=2") {
		t.Fatalf("expected second request to include page=2, got %q", visited[1])
	}

	db, err := storage.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	workCount, err := repo.CountWorks(context.Background())
	if err != nil {
		t.Fatalf("CountWorks() error = %v", err)
	}
	if workCount != 2 {
		t.Fatalf("expected 2 works, got %d", workCount)
	}

	entryCount, err := repo.CountListEntries(context.Background())
	if err != nil {
		t.Fatalf("CountListEntries() error = %v", err)
	}
	if entryCount != 2 {
		t.Fatalf("expected 2 list entries, got %d", entryCount)
	}

	latestCount, err := repo.CountLatestListEntries(context.Background())
	if err != nil {
		t.Fatalf("CountLatestListEntries() error = %v", err)
	}
	if latestCount != 2 {
		t.Fatalf("expected 2 latest list entries, got %d", latestCount)
	}

	work, err := repo.GetWorkByHref(context.Background(), "https://www.metacritic.com/game/alpha")
	if err != nil {
		t.Fatalf("GetWorkByHref() error = %v", err)
	}
	if work.Href == "" {
		t.Fatal("expected stored work href")
	}

	latestEntry, err := repo.GetLatestListEntry(context.Background(), domain.ListEntry{
		WorkHref: "https://www.metacritic.com/game/alpha",
		Category: "game",
		Metric:   "metascore",
		FilterKey: domain.Filter{
			ReleaseYearMin: intPtr(2011),
			ReleaseYearMax: intPtr(2014),
			Platforms:      []string{"pc", "ps5"},
			Genres:         []string{"action", "rpg"},
			ReleaseTypes:   []string{"coming-soon"},
		}.Key(),
	})
	if err != nil {
		t.Fatalf("GetLatestListEntry() error = %v", err)
	}
	if latestEntry.WorkHref == "" {
		t.Fatal("expected latest list entry to be stored")
	}
	if latestEntry.SourceCrawlRunID != result.RunID {
		t.Fatalf("expected latest entry run id %q, got %q", result.RunID, latestEntry.SourceCrawlRunID)
	}

	run, err := repo.GetCrawlRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetCrawlRun() error = %v", err)
	}
	if run.Status != "completed" {
		t.Fatalf("expected crawl run status completed, got %q", run.Status)
	}
}

func TestListServiceRunFailsOnParseFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(invalidCardHTML))
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "failed.db")
	service := NewListService(ListServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 1,
	})

	result, err := service.Run(context.Background(), domain.ListTask{
		Category: "game",
		Metric:   "metascore",
		MaxPages: 1,
	})
	if err == nil {
		t.Fatal("expected Run() to fail")
	}
	if result.RunID == "" {
		t.Fatal("expected failed result to keep run id")
	}
	if result.Failures != 1 {
		t.Fatalf("expected 1 failure, got %d", result.Failures)
	}
	if result.PagesScheduled != 1 || result.PagesSucceeded != 1 || result.PagesWritten != 0 || result.PagesVisited != 1 {
		t.Fatalf("unexpected page stats: %+v", result)
	}
	if result.WorksUpserted != 0 || result.ListEntriesInserted != 0 || result.LatestEntriesUpserted != 0 {
		t.Fatalf("expected no writes, got %+v", result)
	}

	db, err := storage.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	run, err := repo.GetCrawlRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetCrawlRun() error = %v", err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected crawl run status failed, got %q", run.Status)
	}
	if !run.ErrorMessage.Valid || !strings.Contains(run.ErrorMessage.String, "parse list item failed") {
		t.Fatalf("expected parse failure message, got %+v", run.ErrorMessage)
	}
}

func TestListServiceRunTracksFailedPageStats(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/browse/game/":
			if r.URL.Query().Get("page") == "2" {
				http.Error(w, "nope", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(pageOneHTML))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "partial.db")
	service := NewListService(ListServiceConfig{
		BaseURL:    server.URL,
		DBPath:     dbPath,
		MaxRetries: 0,
	})

	result, err := service.Run(context.Background(), domain.ListTask{
		Category: "game",
		Metric:   "metascore",
		MaxPages: 2,
	})
	if err == nil {
		t.Fatal("expected Run() to fail")
	}
	if result.PagesScheduled != 2 || result.PagesSucceeded != 1 || result.PagesWritten != 1 || result.PagesVisited != 1 {
		t.Fatalf("unexpected page stats: %+v", result)
	}
	if result.WorksUpserted != 1 || result.ListEntriesInserted != 1 || result.LatestEntriesUpserted != 1 {
		t.Fatalf("expected first page writes only, got %+v", result)
	}
	if result.Failures != 1 {
		t.Fatalf("expected 1 failure, got %d", result.Failures)
	}

	db, err := storage.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	run, err := repo.GetCrawlRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetCrawlRun() error = %v", err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected crawl run status failed, got %q", run.Status)
	}
}

func TestListServiceRunWithAPISource(t *testing.T) {
	t.Parallel()

	requests := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.String()
		if r.URL.Path != "/finder/metacritic/web" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{
			"data": {
				"totalResults": 1,
				"items": [
					{
						"title": "Alpha",
						"slug": "alpha",
						"releaseDate": "2026-04-23",
						"criticScoreSummary": {"score": "91", "reviewCount": 12},
						"userScore": {"score": "8.4", "reviewCount": 34},
						"image": {"path": "/games/alpha.jpg"}
					}
				]
			},
			"links": {"last": {"meta": {"pageNum": 1}}}
		}`))
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "list-api.db")
	service := NewListService(ListServiceConfig{
		BaseURL:        server.URL,
		BackendBaseURL: server.URL,
		Source:         config.CrawlSourceAPI,
		DBPath:         dbPath,
		MaxRetries:     0,
	})

	result, err := service.Run(context.Background(), domain.ListTask{
		Category: "game",
		Metric:   "metascore",
		MaxPages: 1,
		Filter: domain.Filter{
			Platforms: []string{"pc"},
			Genres:    []string{"action"},
		},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.PagesVisited != 1 || result.PagesWritten != 1 || result.WorksUpserted != 1 {
		t.Fatalf("unexpected api result: %+v", result)
	}

	close(requests)
	var visited []string
	for req := range requests {
		visited = append(visited, req)
	}
	if len(visited) != 1 {
		t.Fatalf("expected exactly one api request, got %+v", visited)
	}
	if !strings.Contains(visited[0], "mcoTypeId=13") || !strings.Contains(visited[0], "sortBy=-metaScore") || !strings.Contains(visited[0], "gamePlatformIds=1500000019") {
		t.Fatalf("expected finder api query in %q", visited[0])
	}
}

func TestListServiceRunAutoFallsBackToHTML(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/finder/metacritic/web":
			http.Error(w, "boom", http.StatusInternalServerError)
		case "/browse/game/":
			_, _ = w.Write([]byte(pageOneHTML))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "list-auto.db")
	service := NewListService(ListServiceConfig{
		BaseURL:        server.URL,
		BackendBaseURL: server.URL,
		Source:         config.CrawlSourceAuto,
		DBPath:         dbPath,
		MaxRetries:     0,
	})

	result, err := service.Run(context.Background(), domain.ListTask{
		Category: "game",
		Metric:   "metascore",
		MaxPages: 1,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.PagesVisited != 1 || result.WorksUpserted != 1 || result.Failures != 0 {
		t.Fatalf("unexpected auto fallback result: %+v", result)
	}
}

func intPtr(v int) *int {
	return &v
}

const pageOneHTML = `
<html>
  <body>
    <div data-testid="filter-results">
      <a href="/game/alpha"><img src="/images/alpha.jpg" /></a>
      <h3 data-testid="product-title"><span>#1</span><span>Alpha</span></h3>
      <div aria-label="Metascore"><span>91</span></div>
      <div aria-label="User Score"><span>8.4</span></div>
      <p>Apr 23, 2026</p>
    </div>
    <nav data-testid="navigation-pagination">
      <span class="c-navigation-pagination__page"><span class="c-navigation-pagination__item-content">1</span></span>
      <span class="c-navigation-pagination__page"><span class="c-navigation-pagination__item-content">2</span></span>
    </nav>
  </body>
</html>
`

const pageTwoHTML = `
<html>
  <body>
    <div data-testid="filter-results">
      <a href="/game/beta"><img src="/images/beta.jpg" /></a>
      <h3 data-testid="product-title"><span>#2</span><span>Beta</span></h3>
      <div aria-label="Metascore"><span>87</span></div>
      <div aria-label="User Score"><span>7.9</span></div>
      <p>Apr 24, 2026</p>
    </div>
  </body>
</html>
`

const invalidCardHTML = `
<html>
  <body>
    <div data-testid="filter-results">
      <a href="/game/broken"><img src="/images/broken.jpg" /></a>
    </div>
  </body>
</html>
`
