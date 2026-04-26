package serve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GoFurry/metacritic-harvester/internal/config"
	"github.com/GoFurry/metacritic-harvester/internal/domain"
	"github.com/GoFurry/metacritic-harvester/internal/storage"
)

type stubTaskDispatcher struct {
	listTask   TaskView
	listCalled bool
}

func (s *stubTaskDispatcher) List() []TaskView {
	if s.listTask.ID == "" {
		return nil
	}
	return []TaskView{s.listTask}
}

func (s *stubTaskDispatcher) Get(id string) (TaskView, bool) {
	if s.listTask.ID == id {
		return s.listTask, true
	}
	return TaskView{}, false
}

func (s *stubTaskDispatcher) SubmitList(config.ListCommandConfig) (TaskView, error) {
	s.listCalled = true
	if s.listTask.ID == "" {
		s.listTask = TaskView{ID: "task-list-1", Kind: "list", Status: TaskStatusPending}
	}
	return s.listTask, nil
}

func (s *stubTaskDispatcher) SubmitDetail(config.DetailCommandConfig) (TaskView, error) {
	return TaskView{ID: "task-detail-1", Kind: "detail", Status: TaskStatusPending}, nil
}

func (s *stubTaskDispatcher) SubmitReview(config.ReviewCommandConfig) (TaskView, error) {
	return TaskView{ID: "task-review-1", Kind: "reviews", Status: TaskStatusPending}, nil
}

func (s *stubTaskDispatcher) SubmitBatch(string, config.BatchRunConfig) (TaskView, error) {
	return TaskView{ID: "task-batch-1", Kind: "batch", Status: TaskStatusPending}, nil
}

func TestServerHealthAndConfig(t *testing.T) {
	srv := NewServer(Config{
		Addr:        "127.0.0.1:9090",
		DBPath:      "output/test.db",
		FullStack:   true,
		EnableWrite: false,
	})

	handler := srv.Handler()

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRec := httptest.NewRecorder()
	handler.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /healthz, got %d", healthRec.Code)
	}

	var health healthView
	if err := json.Unmarshal(healthRec.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if !health.OK {
		t.Fatalf("expected health OK=true")
	}

	configReq := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	configRec := httptest.NewRecorder()
	handler.ServeHTTP(configRec, configReq)
	if configRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /api/config, got %d", configRec.Code)
	}

	var cfgView configView
	if err := json.Unmarshal(configRec.Body.Bytes(), &cfgView); err != nil {
		t.Fatalf("decode config response: %v", err)
	}
	if cfgView.Addr != "127.0.0.1:9090" || !cfgView.FullStack || cfgView.EnableWrite {
		t.Fatalf("unexpected config response: %+v", cfgView)
	}
}

func TestServerLatestEndpoint(t *testing.T) {
	dbPath := seedLatestDB(t)
	srv := NewServer(Config{DBPath: dbPath})

	req := httptest.NewRequest(http.MethodGet, "/api/latest?category=game&metric=metascore&limit=5", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /api/latest, got %d: %s", rec.Code, rec.Body.String())
	}

	var rows []latestEntryView
	if err := json.Unmarshal(rec.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode latest response: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 latest row, got %d", len(rows))
	}
	if rows[0].WorkHref != "https://www.metacritic.com/game/test-game/" {
		t.Fatalf("unexpected work href: %+v", rows[0])
	}
}

func TestServerWriteEndpointDisabled(t *testing.T) {
	srv := NewServer(Config{EnableWrite: false})
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/list", http.NoBody)
	req.RemoteAddr = "127.0.0.1:5050"
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when writes are disabled, got %d", rec.Code)
	}
}

func TestServerWriteEndpointAllowsLoopbackWhenEnabled(t *testing.T) {
	dispatcher := &stubTaskDispatcher{}
	srv := NewServer(Config{EnableWrite: true})
	srv.tasks = dispatcher

	body := `{"category":"game","metric":"metascore","pages":1,"source":"api"}`
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/list", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:5050"
	rec := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for accepted list task, got %d: %s", rec.Code, rec.Body.String())
	}
	if !dispatcher.listCalled {
		t.Fatalf("expected list task dispatcher to be called")
	}
}

func TestServerLatestExportEndpoint(t *testing.T) {
	dbPath := seedLatestDB(t)
	srv := NewServer(Config{DBPath: dbPath})

	req := httptest.NewRequest(http.MethodGet, "/api/export/latest?category=game&metric=metascore&format=csv&profile=summary", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /api/export/latest, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Disposition"); !strings.Contains(got, "attachment;") {
		t.Fatalf("expected attachment header, got %q", got)
	}
	if !strings.Contains(rec.Body.String(), "run_id,category,metric") {
		t.Fatalf("expected CSV header in export body, got %q", rec.Body.String())
	}
}

func TestServerDetailExportEndpoint(t *testing.T) {
	dbPath := seedDetailDB(t)
	srv := NewServer(Config{DBPath: dbPath})

	req := httptest.NewRequest(http.MethodGet, "/api/export/detail?category=game&format=json&profile=flat", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /api/export/detail, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Disposition"); !strings.Contains(got, "attachment;") {
		t.Fatalf("expected attachment header, got %q", got)
	}
	if !strings.Contains(rec.Body.String(), "\"work_href\"") {
		t.Fatalf("expected JSON export body, got %q", rec.Body.String())
	}
}

func TestServerReviewExportEndpoint(t *testing.T) {
	dbPath := seedReviewDB(t)
	srv := NewServer(Config{DBPath: dbPath})

	req := httptest.NewRequest(http.MethodGet, "/api/export/review?category=game&review_type=critic&format=csv&profile=summary", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /api/export/review, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Disposition"); !strings.Contains(got, "attachment;") {
		t.Fatalf("expected attachment header, got %q", got)
	}
	if !strings.Contains(rec.Body.String(), "run_id,category,review_type") {
		t.Fatalf("expected CSV header in review export body, got %q", rec.Body.String())
	}
}

func TestServerOverviewEndpoint(t *testing.T) {
	dbPath := seedLatestDB(t)
	srv := NewServer(Config{DBPath: dbPath})

	req := httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /api/overview, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "\"exports\"") || !strings.Contains(rec.Body.String(), "\"runs\"") {
		t.Fatalf("expected overview payload, got %s", rec.Body.String())
	}
}

func seedLatestDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "serve-test.db")
	ctx := context.Background()
	db, err := storage.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	task := domain.ListTask{
		Category: domain.CategoryGame,
		Metric:   domain.MetricMetascore,
		Filter:   domain.Filter{},
		MaxPages: 1,
	}
	startedAt := time.Now().UTC()
	if err := repo.CreateCrawlRun(ctx, "run-list-1", "test", "test-list", task, startedAt); err != nil {
		t.Fatalf("create crawl run: %v", err)
	}

	err = repo.SaveListEntrySnapshots(ctx, []storage.ListEntrySnapshot{{
		Work: domain.Work{
			Name:        "Test Game",
			Href:        "https://www.metacritic.com/game/test-game/",
			ReleaseDate: "2025-01-01",
			Category:    domain.CategoryGame,
		},
		Entry: domain.ListEntry{
			CrawlRunID: "run-list-1",
			WorkHref:   "https://www.metacritic.com/game/test-game/",
			Category:   domain.CategoryGame,
			Metric:     domain.MetricMetascore,
			Page:       1,
			Rank:       1,
			Metascore:  "95",
			FilterKey:  "",
			CrawledAt:  startedAt,
		},
	}})
	if err != nil {
		t.Fatalf("save list snapshots: %v", err)
	}

	return dbPath
}

func seedDetailDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "serve-detail.db")
	ctx := context.Background()
	db, err := storage.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open detail database: %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	workHref := "https://www.metacritic.com/game/test-detail/"
	if _, err := db.ExecContext(ctx, "INSERT INTO works (href, name, category) VALUES (?, ?, ?)", workHref, "Test Detail", "game"); err != nil {
		t.Fatalf("insert work: %v", err)
	}
	startedAt := time.Now().UTC()
	if err := repo.CreateDetailCrawlRun(ctx, "detail-run-1", "game", "detail-game", "href=all|force=0|limit=all", startedAt); err != nil {
		t.Fatalf("CreateDetailCrawlRun() error = %v", err)
	}
	if err := repo.SaveWorkDetail(ctx, domain.WorkDetail{
		WorkHref:             workHref,
		Category:             domain.CategoryGame,
		Title:                "Test Detail",
		Summary:              "Detail summary",
		ReleaseDate:          "2025-02-01",
		Metascore:            "91",
		MetascoreSentiment:   "Universal Acclaim",
		MetascoreReviewCount: 12,
		UserScore:            "8.8",
		UserScoreSentiment:   "Generally Favorable",
		UserScoreCount:       88,
		Rating:               "M",
		Duration:             "",
		Tagline:              "",
		Details: domain.WorkDetailExtras{
			Genres:     []string{"Action", "RPG"},
			Platforms:  []string{"PC"},
			Developers: []string{"Larian"},
		},
		LastFetchedAt: startedAt,
	}, startedAt, "detail-run-1"); err != nil {
		t.Fatalf("SaveWorkDetail() error = %v", err)
	}
	return dbPath
}

func seedReviewDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "serve-review.db")
	ctx := context.Background()
	db, err := storage.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open review database: %v", err)
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	workHref := "https://www.metacritic.com/game/test-review/"
	if _, err := db.ExecContext(ctx, "INSERT INTO works (href, name, category) VALUES (?, ?, ?)", workHref, "Test Review", "game"); err != nil {
		t.Fatalf("insert work: %v", err)
	}
	startedAt := time.Now().UTC()
	if err := repo.CreateReviewCrawlRun(ctx, "review-run-1", "crawl reviews", "reviews-game", "game", "category=game", startedAt); err != nil {
		t.Fatalf("CreateReviewCrawlRun() error = %v", err)
	}
	score := 92.0
	record := domain.ReviewRecord{
		ReviewKey:         domain.BuildCriticReviewKey(workHref, domain.CategoryGame, "pc", "pc-gamer", "2025-02-01", "Excellent."),
		CrawlRunID:        "review-run-1",
		WorkHref:          workHref,
		Category:          domain.CategoryGame,
		ReviewType:        domain.ReviewTypeCritic,
		PlatformKey:       "pc",
		ReviewURL:         "https://example.test/review",
		ReviewDate:        "2025-02-01",
		Score:             &score,
		Quote:             "Excellent.",
		PublicationName:   "PC Gamer",
		PublicationSlug:   "pc-gamer",
		AuthorName:        "Pat",
		AuthorSlug:        "pat",
		SourcePayloadJSON: `{"kind":"critic"}`,
		CrawledAt:         startedAt,
	}
	if err := repo.SaveReviewRecords(ctx, []domain.ReviewRecord{record}); err != nil {
		t.Fatalf("SaveReviewRecords() error = %v", err)
	}
	if err := repo.MarkReviewSucceeded(ctx, domain.ReviewScope{
		WorkHref:    workHref,
		Category:    domain.CategoryGame,
		ReviewType:  domain.ReviewTypeCritic,
		PlatformKey: "pc",
	}, startedAt, startedAt, "review-run-1"); err != nil {
		t.Fatalf("MarkReviewSucceeded() error = %v", err)
	}
	return dbPath
}
