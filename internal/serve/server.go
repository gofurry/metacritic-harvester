package serve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

type Config struct {
	Addr           string
	DBPath         string
	FullStack      bool
	EnableWrite    bool
	BaseURL        string
	BackendBaseURL string
}

type Server struct {
	cfg         Config
	logs        *LogBroker
	tasks       taskDispatcher
	taskManager *TaskManager
	ui          http.Handler
	httpServer  *http.Server
}

func NewServer(cfg Config) *Server {
	if strings.TrimSpace(cfg.Addr) == "" {
		cfg.Addr = "127.0.0.1:36666"
	}
	if strings.TrimSpace(cfg.DBPath) == "" {
		cfg.DBPath = "output/metacritic.db"
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = config.DefaultBaseURL
	}
	if strings.TrimSpace(cfg.BackendBaseURL) == "" {
		cfg.BackendBaseURL = config.DefaultBackendBaseURL
	}
	return &Server{
		cfg:  cfg,
		logs: NewLogBroker(defaultLogBufferSize),
		ui:   uiFileServer(),
	}
}

func (s *Server) Run(ctx context.Context) error {
	s.ensureTasks(ctx)

	origWriter := log.Writer()
	log.SetOutput(io.MultiWriter(origWriter, s.logs.Writer()))
	defer log.SetOutput(origWriter)

	s.httpServer = &http.Server{
		Addr:              s.cfg.Addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	shutdownErrCh := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErrCh <- s.httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("serve listening: addr=%s db=%s full_stack=%t enable_write=%t", s.cfg.Addr, s.cfg.DBPath, s.cfg.FullStack, s.cfg.EnableWrite)
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		select {
		case shutdownErr := <-shutdownErrCh:
			if shutdownErr != nil && !errors.Is(shutdownErr, context.Canceled) {
				return shutdownErr
			}
		default:
		}
		return nil
	}
	return err
}

func (s *Server) Handler() http.Handler {
	s.ensureTasks(context.Background())

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/runs", s.handleRuns)
	mux.HandleFunc("/api/overview", s.handleOverview)
	mux.HandleFunc("/api/tasks", s.handleTasks)
	mux.HandleFunc("/api/tasks/", s.handleTaskByID)
	mux.HandleFunc("/api/logs", s.handleRecentLogs)
	mux.HandleFunc("/api/logs/stream", s.handleLogStream)
	mux.HandleFunc("/api/latest", s.handleLatest)
	mux.HandleFunc("/api/detail", s.handleDetail)
	mux.HandleFunc("/api/review", s.handleReview)
	mux.HandleFunc("/api/export/latest", s.handleLatestExport)
	mux.HandleFunc("/api/export/detail", s.handleDetailExport)
	mux.HandleFunc("/api/export/review", s.handleReviewExport)
	mux.HandleFunc("/api/detail/state", s.handleDetailFetchState)
	mux.HandleFunc("/api/review/state", s.handleReviewFetchState)
	mux.HandleFunc("/api/tasks/list", s.handleSubmitList)
	mux.HandleFunc("/api/tasks/detail", s.handleSubmitDetail)
	mux.HandleFunc("/api/tasks/reviews", s.handleSubmitReview)

	if s.cfg.FullStack {
		mux.Handle("/", s.ui)
	} else {
		mux.HandleFunc("/", s.handleRoot)
	}
	return withJSONRecovery(mux)
}

func (s *Server) ensureTasks(ctx context.Context) {
	if s.tasks != nil {
		return
	}
	s.taskManager = NewTaskManager(ctx, s.cfg)
	s.tasks = s.taskManager
}

func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":      "metacritic-harvester",
		"mode":         "backend",
		"healthz":      "/healthz",
		"api_base":     "/api",
		"full_stack":   false,
		"enable_write": s.cfg.EnableWrite,
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthView{OK: true})
}

func (s *Server) handleConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, configView{
		Addr:        s.cfg.Addr,
		DBPath:      s.cfg.DBPath,
		FullStack:   s.cfg.FullStack,
		EnableWrite: s.cfg.EnableWrite,
	})
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list runs", err)
		return
	}
	defer closeFn()

	limit := parsePositiveQueryInt(r, "limit", 50)
	rows, err := repo.ListCrawlRuns(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list runs", err)
		return
	}
	writeJSON(w, http.StatusOK, mapCrawlRuns(rows))
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	view, err := buildOverview(r.Context(), s.cfg.DBPath, s.tasks.List())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "overview", err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.tasks.List())
	default:
		writeMethodNotAllowed(w, http.MethodGet)
	}
}

func (s *Server) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
	if strings.TrimSpace(id) == "" {
		writeErrorMessage(w, http.StatusBadRequest, "get task", "task id must not be empty")
		return
	}
	task, ok := s.tasks.Get(id)
	if !ok {
		writeErrorMessage(w, http.StatusNotFound, "get task", "task not found")
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) handleRecentLogs(w http.ResponseWriter, r *http.Request) {
	limit := parsePositiveQueryInt(r, "limit", 200)
	writeJSON(w, http.StatusOK, s.logs.Recent(limit))
}

func (s *Server) handleLogStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErrorMessage(w, http.StatusInternalServerError, "stream logs", "streaming is not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for _, event := range s.logs.Recent(50) {
		payload, err := encodeSSEEvent(event)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "stream logs", err)
			return
		}
		if _, err := w.Write(payload); err != nil {
			return
		}
	}
	flusher.Flush()

	events, unsubscribe := s.logs.Subscribe()
	defer unsubscribe()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			payload, err := encodeSSEEvent(event)
			if err != nil {
				return
			}
			if _, err := w.Write(payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query latest", err)
		return
	}
	defer closeFn()

	rows, err := repo.ListLatestEntries(r.Context(), storage.ListLatestEntriesFilter{
		Category:  strings.TrimSpace(r.URL.Query().Get("category")),
		Metric:    strings.TrimSpace(r.URL.Query().Get("metric")),
		WorkHref:  domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL),
		FilterKey: strings.TrimSpace(r.URL.Query().Get("filter_key")),
		Limit:     parsePositiveQueryInt(r, "limit", 100),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query latest", err)
		return
	}
	writeJSON(w, http.StatusOK, mapLatestEntries(rows))
}

func (s *Server) handleDetail(w http.ResponseWriter, r *http.Request) {
	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query detail", err)
		return
	}
	defer closeFn()

	rows, err := repo.ListWorkDetails(r.Context(), storage.ListWorkDetailsFilter{
		Category: strings.TrimSpace(r.URL.Query().Get("category")),
		WorkHref: domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL),
		Limit:    parsePositiveQueryInt(r, "limit", 100),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query detail", err)
		return
	}
	mapped, err := mapWorkDetails(rows)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query detail", err)
		return
	}
	writeJSON(w, http.StatusOK, mapped)
}

func (s *Server) handleReview(w http.ResponseWriter, r *http.Request) {
	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query review", err)
		return
	}
	defer closeFn()

	rows, err := repo.ListLatestReviews(r.Context(), storage.ListLatestReviewsFilter{
		Category:   strings.TrimSpace(r.URL.Query().Get("category")),
		ReviewType: strings.TrimSpace(r.URL.Query().Get("review_type")),
		Platform:   strings.TrimSpace(r.URL.Query().Get("platform")),
		WorkHref:   domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL),
		Limit:      parsePositiveQueryInt(r, "limit", 100),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query review", err)
		return
	}
	writeJSON(w, http.StatusOK, mapLatestReviews(rows))
}

func (s *Server) handleLatestExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	format := strings.TrimSpace(r.URL.Query().Get("format"))
	if format == "" {
		format = exportFormatCSV
	}
	profile := strings.TrimSpace(r.URL.Query().Get("profile"))
	if profile == "" {
		profile = exportProfileRaw
	}
	if !isValidExportFormat(format) || !isValidExportProfile(profile) {
		writeErrorMessage(w, http.StatusBadRequest, "export latest", "format must be csv|json and profile must be raw|flat|summary")
		return
	}
	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "export latest", err)
		return
	}
	defer closeFn()
	download, err := buildLatestExport(r.Context(), repo, storage.ListLatestEntriesFilter{
		Category:  strings.TrimSpace(r.URL.Query().Get("category")),
		Metric:    strings.TrimSpace(r.URL.Query().Get("metric")),
		WorkHref:  domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL),
		FilterKey: strings.TrimSpace(r.URL.Query().Get("filter_key")),
		Limit:     -1,
	}, strings.TrimSpace(r.URL.Query().Get("run_id")), format, profile)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "export latest", err)
		return
	}
	writeDownload(w, download)
}

func (s *Server) handleDetailExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	format := strings.TrimSpace(r.URL.Query().Get("format"))
	if format == "" {
		format = exportFormatCSV
	}
	profile := strings.TrimSpace(r.URL.Query().Get("profile"))
	if profile == "" {
		profile = exportProfileRaw
	}
	if !isValidExportFormat(format) || !isValidExportProfile(profile) {
		writeErrorMessage(w, http.StatusBadRequest, "export detail", "format must be csv|json and profile must be raw|flat|summary")
		return
	}
	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "export detail", err)
		return
	}
	defer closeFn()
	download, err := buildDetailExport(r.Context(), repo, storage.ListWorkDetailsFilter{
		Category: strings.TrimSpace(r.URL.Query().Get("category")),
		WorkHref: domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL),
		Limit:    -1,
	}, strings.TrimSpace(r.URL.Query().Get("run_id")), format, profile)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "export detail", err)
		return
	}
	writeDownload(w, download)
}

func (s *Server) handleReviewExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, http.MethodGet)
		return
	}
	format := strings.TrimSpace(r.URL.Query().Get("format"))
	if format == "" {
		format = exportFormatCSV
	}
	profile := strings.TrimSpace(r.URL.Query().Get("profile"))
	if profile == "" {
		profile = exportProfileRaw
	}
	if !isValidExportFormat(format) || !isValidExportProfile(profile) {
		writeErrorMessage(w, http.StatusBadRequest, "export review", "format must be csv|json and profile must be raw|flat|summary")
		return
	}
	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "export review", err)
		return
	}
	defer closeFn()
	download, err := buildReviewExport(r.Context(), repo, storage.ListLatestReviewsFilter{
		Category:   strings.TrimSpace(r.URL.Query().Get("category")),
		ReviewType: strings.TrimSpace(r.URL.Query().Get("review_type")),
		Platform:   strings.TrimSpace(r.URL.Query().Get("platform")),
		WorkHref:   domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL),
		Limit:      -1,
	}, strings.TrimSpace(r.URL.Query().Get("run_id")), format, profile)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "export review", err)
		return
	}
	writeDownload(w, download)
}

func (s *Server) handleDetailFetchState(w http.ResponseWriter, r *http.Request) {
	workHref := domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL)
	if strings.TrimSpace(workHref) == "" {
		writeErrorMessage(w, http.StatusBadRequest, "detail fetch state", "work_href must not be empty")
		return
	}

	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "detail fetch state", err)
		return
	}
	defer closeFn()

	row, err := repo.GetDetailFetchState(r.Context(), workHref)
	if err != nil {
		if storage.IsNotFound(err) {
			writeJSON(w, http.StatusOK, map[string]any{})
			return
		}
		writeError(w, http.StatusInternalServerError, "detail fetch state", err)
		return
	}
	writeJSON(w, http.StatusOK, mapDetailFetchState(row))
}

func (s *Server) handleReviewFetchState(w http.ResponseWriter, r *http.Request) {
	workHref := domain.NormalizeWorkHref(r.URL.Query().Get("work_href"), config.DefaultBaseURL)
	reviewType := strings.TrimSpace(r.URL.Query().Get("review_type"))
	platform := strings.TrimSpace(r.URL.Query().Get("platform"))
	if strings.TrimSpace(workHref) == "" || reviewType == "" {
		writeErrorMessage(w, http.StatusBadRequest, "review fetch state", "work_href and review_type must not be empty")
		return
	}
	parsedType, err := domain.ParseReviewType(reviewType)
	if err != nil || parsedType == domain.ReviewTypeAll {
		writeErrorMessage(w, http.StatusBadRequest, "review fetch state", "review_type must be critic or user")
		return
	}

	repo, closeFn, err := openReadRepository(r.Context(), s.cfg.DBPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "review fetch state", err)
		return
	}
	defer closeFn()

	row, err := repo.GetReviewFetchState(r.Context(), domain.ReviewScope{
		WorkHref:    workHref,
		ReviewType:  parsedType,
		PlatformKey: platform,
	})
	if err != nil {
		if storage.IsNotFound(err) {
			writeJSON(w, http.StatusOK, map[string]any{})
			return
		}
		writeError(w, http.StatusInternalServerError, "review fetch state", err)
		return
	}
	writeJSON(w, http.StatusOK, mapReviewFetchState(row))
}

type listTaskRequest struct {
	Category        string  `json:"category"`
	Metric          string  `json:"metric"`
	Source          string  `json:"source"`
	Year            string  `json:"year"`
	Platform        string  `json:"platform"`
	Network         string  `json:"network"`
	Genre           string  `json:"genre"`
	ReleaseType     string  `json:"release_type"`
	Pages           int     `json:"pages"`
	Timeout         string  `json:"timeout"`
	ContinueOnError *bool   `json:"continue_on_error"`
	RPS             float64 `json:"rps"`
	Burst           int     `json:"burst"`
	Debug           bool    `json:"debug"`
	Retries         int     `json:"retries"`
	Proxies         string  `json:"proxies"`
}

func (s *Server) handleSubmitList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if !s.authorizeWriteRequest(w, r, "submit list task") {
		return
	}

	var req listTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "submit list task", err)
		return
	}

	timeout, err := parseOptionalDuration(req.Timeout, config.DefaultCrawlCommandTimeout)
	if err != nil {
		writeError(w, http.StatusBadRequest, "submit list task", err)
		return
	}

	cfg, err := config.BuildListCommandConfig(config.ListCommandOptions{
		Category:        req.Category,
		Metric:          req.Metric,
		Source:          req.Source,
		Year:            req.Year,
		Platform:        req.Platform,
		Network:         req.Network,
		Genre:           req.Genre,
		ReleaseType:     req.ReleaseType,
		Pages:           defaultZeroOrPositive(req.Pages, 1),
		DBPath:          s.cfg.DBPath,
		Debug:           req.Debug,
		Timeout:         timeout,
		ContinueOnError: defaultBool(req.ContinueOnError, true),
		RPS:             defaultFloat64(req.RPS, config.DefaultCrawlRateRPS),
		Burst:           defaultPositive(req.Burst, config.DefaultCrawlRateBurst),
		MaxRetries:      defaultNonNegative(req.Retries, 3),
		Proxies:         req.Proxies,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "submit list task", err)
		return
	}

	task, err := s.tasks.SubmitList(cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "submit list task", err)
		return
	}
	writeJSON(w, http.StatusAccepted, task)
}

type detailTaskRequest struct {
	Category        string  `json:"category"`
	WorkHref        string  `json:"work_href"`
	Source          string  `json:"source"`
	Limit           int     `json:"limit"`
	Force           bool    `json:"force"`
	Concurrency     int     `json:"concurrency"`
	Timeout         string  `json:"timeout"`
	ContinueOnError *bool   `json:"continue_on_error"`
	RPS             float64 `json:"rps"`
	Burst           int     `json:"burst"`
	Debug           bool    `json:"debug"`
	Retries         int     `json:"retries"`
	Proxies         string  `json:"proxies"`
}

func (s *Server) handleSubmitDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if !s.authorizeWriteRequest(w, r, "submit detail task") {
		return
	}

	var req detailTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "submit detail task", err)
		return
	}

	timeout, err := parseOptionalDuration(req.Timeout, config.DefaultCrawlCommandTimeout)
	if err != nil {
		writeError(w, http.StatusBadRequest, "submit detail task", err)
		return
	}

	cfg, err := config.BuildDetailCommandConfig(config.DetailCommandOptions{
		Category:        req.Category,
		WorkHref:        req.WorkHref,
		Source:          req.Source,
		Limit:           defaultNonNegative(req.Limit, 0),
		Force:           req.Force,
		DBPath:          s.cfg.DBPath,
		Debug:           req.Debug,
		Timeout:         timeout,
		ContinueOnError: defaultBool(req.ContinueOnError, true),
		RPS:             defaultFloat64(req.RPS, config.DefaultCrawlRateRPS),
		Burst:           defaultPositive(req.Burst, config.DefaultCrawlRateBurst),
		MaxRetries:      defaultNonNegative(req.Retries, 3),
		Proxies:         req.Proxies,
		Concurrency:     defaultPositive(req.Concurrency, 1),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "submit detail task", err)
		return
	}

	task, err := s.tasks.SubmitDetail(cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "submit detail task", err)
		return
	}
	writeJSON(w, http.StatusAccepted, task)
}

type reviewTaskRequest struct {
	Category        string  `json:"category"`
	WorkHref        string  `json:"work_href"`
	Limit           int     `json:"limit"`
	Force           bool    `json:"force"`
	Concurrency     int     `json:"concurrency"`
	ReviewType      string  `json:"review_type"`
	Sentiment       string  `json:"sentiment"`
	Sort            string  `json:"sort"`
	Platform        string  `json:"platform"`
	PageSize        int     `json:"page_size"`
	MaxPages        int     `json:"max_pages"`
	Timeout         string  `json:"timeout"`
	ContinueOnError *bool   `json:"continue_on_error"`
	RPS             float64 `json:"rps"`
	Burst           int     `json:"burst"`
	Debug           bool    `json:"debug"`
	Retries         int     `json:"retries"`
	Proxies         string  `json:"proxies"`
}

func (s *Server) handleSubmitReview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, http.MethodPost)
		return
	}
	if !s.authorizeWriteRequest(w, r, "submit review task") {
		return
	}

	var req reviewTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "submit review task", err)
		return
	}

	timeout, err := parseOptionalDuration(req.Timeout, config.DefaultCrawlCommandTimeout)
	if err != nil {
		writeError(w, http.StatusBadRequest, "submit review task", err)
		return
	}

	cfg, err := config.BuildReviewCommandConfig(config.ReviewCommandOptions{
		Category:        req.Category,
		WorkHref:        req.WorkHref,
		Limit:           defaultNonNegative(req.Limit, 0),
		Force:           req.Force,
		Concurrency:     defaultPositive(req.Concurrency, 1),
		ReviewType:      firstNonEmptyString(req.ReviewType, "all"),
		Sentiment:       firstNonEmptyString(req.Sentiment, "all"),
		Sort:            req.Sort,
		Platform:        req.Platform,
		PageSize:        defaultPositive(req.PageSize, 20),
		MaxPages:        defaultNonNegative(req.MaxPages, 0),
		DBPath:          s.cfg.DBPath,
		Debug:           req.Debug,
		Timeout:         timeout,
		ContinueOnError: defaultBool(req.ContinueOnError, true),
		RPS:             defaultFloat64(req.RPS, config.DefaultCrawlRateRPS),
		Burst:           defaultPositive(req.Burst, config.DefaultCrawlRateBurst),
		MaxRetries:      defaultNonNegative(req.Retries, 3),
		Proxies:         req.Proxies,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "submit review task", err)
		return
	}

	task, err := s.tasks.SubmitReview(cfg)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "submit review task", err)
		return
	}
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) authorizeWriteRequest(w http.ResponseWriter, r *http.Request, op string) bool {
	if !s.cfg.EnableWrite {
		writeErrorMessage(w, http.StatusForbidden, op, "write endpoints are disabled")
		return false
	}
	if !isLoopbackRequest(r) {
		writeErrorMessage(w, http.StatusForbidden, op, "write endpoints only accept loopback requests")
		return false
	}
	return true
}

func isLoopbackRequest(r *http.Request) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		host = strings.TrimSpace(r.RemoteAddr)
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func openReadRepository(ctx context.Context, dbPath string) (*storage.Repository, func() error, error) {
	db, err := storage.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return nil, nil, err
	}
	return storage.NewRepository(db), db.Close, nil
}

func withJSONRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeErrorMessage(w, http.StatusInternalServerError, "http handler", fmt.Sprintf("panic: %v", rec))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeMethodNotAllowed(w http.ResponseWriter, methods ...string) {
	if len(methods) > 0 {
		w.Header().Set("Allow", strings.Join(methods, ", "))
	}
	writeErrorMessage(w, http.StatusMethodNotAllowed, "http method", "method not allowed")
}

func writeError(w http.ResponseWriter, status int, op string, err error) {
	writeErrorMessage(w, status, op, err.Error())
}

func writeErrorMessage(w http.ResponseWriter, status int, op string, message string) {
	writeJSON(w, status, map[string]any{
		"error":   message,
		"op":      op,
		"status":  status,
		"success": false,
	})
}

func parsePositiveQueryInt(r *http.Request, key string, defaultValue int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return defaultValue
	}
	return value
}

func writeDownload(w http.ResponseWriter, download exportDownload) {
	w.Header().Set("Content-Type", download.ContentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", download.Filename))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(download.Body)
}

func defaultPositive(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func defaultNonNegative(value int, fallback int) int {
	if value >= 0 {
		return value
	}
	return fallback
}

func defaultZeroOrPositive(value int, fallback int) int {
	if value >= 0 {
		return value
	}
	return fallback
}

func defaultFloat64(value float64, fallback float64) float64 {
	if value > 0 {
		return value
	}
	return fallback
}

func defaultBool(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func parseOptionalDuration(raw string, fallback time.Duration) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", raw)
	}
	return duration, nil
}
