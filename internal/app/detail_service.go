package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/google/uuid"

	"github.com/GoFurry/metacritic-harvester/internal/config"
	"github.com/GoFurry/metacritic-harvester/internal/crawler"
	"github.com/GoFurry/metacritic-harvester/internal/domain"
	"github.com/GoFurry/metacritic-harvester/internal/source/metacritic"
	detailapi "github.com/GoFurry/metacritic-harvester/internal/source/metacritic/api"
	"github.com/GoFurry/metacritic-harvester/internal/storage"
)

const detailStaleRunningThreshold = 15 * time.Minute

const (
	detailErrorTypeState          = "state"
	detailErrorTypeContext        = "context"
	detailErrorTypeNetwork        = "network"
	detailErrorTypeHTTP403        = "http_403"
	detailErrorTypeHTTP404        = "http_404"
	detailErrorTypeHTTP429        = "http_429"
	detailErrorTypeHTTP5xx        = "http_5xx"
	detailErrorTypeParse          = "parse"
	detailErrorTypeWrite          = "write"
	detailErrorTypeStateRecovered = "state_recovered"
)

const (
	detailErrorStageRecovery    = "recovery"
	detailErrorStageMarkRunning = "mark_running"
	detailErrorStageRequest     = "request"
	detailErrorStageParse       = "parse"
	detailErrorStageSave        = "save"
	detailErrorStageContext     = "context"
)

type DetailServiceConfig struct {
	BaseURL        string
	BackendBaseURL string
	Source         config.CrawlSource
	DBPath         string
	Debug          bool
	MaxRetries     int
	ProxyURLs      []string
}

type DetailRunResult struct {
	RunID            string
	Total            int
	Processed        int
	Fetched          int
	Skipped          int
	Failed           int
	RecoveredRunning int
	DetailsUpserted  int
	Failures         int
}

type DetailService struct {
	cfg   DetailServiceConfig
	now   func() time.Time
	sleep func(time.Duration)
}

func NewDetailService(cfg DetailServiceConfig) *DetailService {
	return &DetailService{
		cfg:   cfg,
		now:   time.Now,
		sleep: time.Sleep,
	}
}

func (s *DetailService) normalizedSource() config.CrawlSource {
	switch s.cfg.Source {
	case config.CrawlSourceAPI, config.CrawlSourceHTML, config.CrawlSourceAuto:
		return s.cfg.Source
	default:
		return config.CrawlSourceHTML
	}
}

func (s *DetailService) backendBaseURL() string {
	baseURL := strings.TrimSpace(s.cfg.BackendBaseURL)
	if baseURL == "" {
		return config.DefaultBackendBaseURL
	}
	return baseURL
}

func (s *DetailService) Run(ctx context.Context, task domain.DetailTask) (DetailRunResult, error) {
	db, err := storage.Open(ctx, s.cfg.DBPath)
	if err != nil {
		return DetailRunResult{}, err
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	scope := buildDetailRunScope(task)
	runID := uuid.NewString()
	startedAt := s.now().UTC()
	if err := repo.CreateDetailCrawlRun(ctx, runID, scope.Category, scope.TaskName, scope.FilterKey, startedAt); err != nil {
		return DetailRunResult{}, err
	}

	result := DetailRunResult{RunID: runID}
	var finalErr error
	defer func() {
		finishedAt := s.now().UTC()
		if finalErr != nil {
			_ = repo.FailDetailCrawlRun(context.Background(), runID, finishedAt, finalErr.Error())
			return
		}
		_ = repo.CompleteDetailCrawlRun(context.Background(), runID, finishedAt)
	}()

	workerCount := task.Concurrency
	if workerCount <= 0 {
		workerCount = 1
	}

	log.Printf(
		"crawl detail start: run_id=%s scope=%s db=%s concurrency=%d",
		runID,
		scope.Label,
		s.cfg.DBPath,
		workerCount,
	)

	staleBefore := s.now().UTC().Add(-detailStaleRunningThreshold)
	recoveredRunning, err := repo.RecoverStaleDetailFetchStates(ctx, storage.ListDetailCandidatesFilter{
		Category: string(task.Category),
		WorkHref: task.WorkHref,
	}, staleBefore, runID)
	if err != nil {
		finalErr = detailFailuref(detailErrorTypeState, detailErrorStageRecovery, err, "detail recovery failed: run_id=%s scope=%s", runID, scope.Label)
		return result, finalErr
	}
	result.RecoveredRunning = int(recoveredRunning)
	if recoveredRunning > 0 {
		log.Printf("crawl detail recovery: run_id=%s scope=%s recovered_running=%d stale_before=%s", runID, scope.Label, recoveredRunning, staleBefore.Format(time.RFC3339))
	}

	candidates, err := repo.ListDetailCandidates(ctx, storage.ListDetailCandidatesFilter{
		Category: string(task.Category),
		WorkHref: task.WorkHref,
		Limit:    task.Limit,
		Force:    task.Force,
	})
	if err != nil {
		finalErr = detailFailuref(detailErrorTypeState, detailErrorStageRecovery, err, "list detail candidates failed: run_id=%s scope=%s", runID, scope.Label)
		return result, finalErr
	}

	state := &detailRunState{
		result: DetailRunResult{
			RunID:            runID,
			Total:            len(candidates),
			RecoveredRunning: int(recoveredRunning),
		},
	}

	fetchers, err := s.buildDetailFetchers(workerCount)
	if err != nil {
		finalErr = detailFailuref(detailErrorTypeState, detailErrorStageRequest, err, "create detail worker fetchers failed: run_id=%s scope=%s", runID, scope.Label)
		return state.snapshot(), finalErr
	}

	jobs := make(chan storage.DetailCandidate)
	var wg sync.WaitGroup
	var dbWriteMu sync.Mutex

	for _, fetcher := range fetchers {
		wg.Add(1)
		go func(fetcher detailFetcher) {
			defer wg.Done()
			for candidate := range jobs {
				s.processDetailCandidate(ctx, repo, fetcher, candidate, staleBefore, runID, state, &dbWriteMu)
			}
		}(fetcher)
	}

sendJobs:
	for _, candidate := range candidates {
		select {
		case <-ctx.Done():
			state.setFirstErr(detailFailuref(detailErrorTypeContext, detailErrorStageContext, ctx.Err(), "detail crawl canceled: run_id=%s scope=%s", runID, scope.Label))
			break sendJobs
		case jobs <- candidate:
		}
	}
	close(jobs)
	wg.Wait()

	result = state.snapshot()
	if err := state.firstError(); err != nil {
		finalErr = fmt.Errorf("detail crawl run %s completed with %d failure(s): %w", runID, result.Failures, err)
		return result, finalErr
	}

	if err := ctx.Err(); err != nil {
		finalErr = detailFailuref(detailErrorTypeContext, detailErrorStageContext, err, "detail crawl canceled: run_id=%s scope=%s", runID, scope.Label)
		return result, finalErr
	}

	log.Printf(
		"crawl detail finished successfully: run_id=%s processed=%d/%d fetched=%d skipped=%d failed=%d recovered_running=%d db=%s",
		runID,
		result.Processed,
		result.Total,
		result.Fetched,
		result.Skipped,
		result.Failed,
		result.RecoveredRunning,
		s.cfg.DBPath,
	)
	return result, nil
}

func (s *DetailService) processDetailCandidate(
	ctx context.Context,
	repo *storage.Repository,
	fetcher detailFetcher,
	candidate storage.DetailCandidate,
	staleBefore time.Time,
	runID string,
	state *detailRunState,
	dbWriteMu *sync.Mutex,
) {
	workLabel := detailWorkLabel(candidate.Work)

	if err := ctx.Err(); err != nil {
		failure := detailFailuref(detailErrorTypeContext, detailErrorStageContext, err, "detail crawl canceled: run_id=%s href=%s", runID, candidate.Work.Href)
		snapshot := state.recordFailure(failure)
		log.Printf("detail failed: run_id=%s %s error_type=%s error_stage=%s error=%v", runID, workLabel, failure.Type, failure.Stage, failure)
		logDetailProgress(runID, snapshot)
		return
	}

	if fresh, stateErr := isFreshRunningCandidate(candidate, staleBefore); stateErr != nil {
		failure := detailFailuref(detailErrorTypeState, detailErrorStageRecovery, stateErr, "detail state invalid: run_id=%s href=%s", runID, candidate.Work.Href)
		snapshot := state.recordFailure(failure)
		log.Printf("detail failed: run_id=%s %s error_type=%s error_stage=%s error=%v", runID, workLabel, detailErrorTypeState, detailErrorStageRecovery, failure)
		logDetailProgress(runID, snapshot)
		return
	} else if fresh {
		snapshot := state.recordSkip()
		log.Printf("detail skipped: run_id=%s %s reason=fresh_running last_attempted_at=%s", runID, workLabel, candidate.LastAttemptedAt)
		logDetailProgress(runID, snapshot)
		return
	}

	attemptedAt := s.now().UTC()
	log.Printf("detail start: run_id=%s %s", runID, workLabel)

	dbWriteMu.Lock()
	err := repo.MarkDetailRunning(ctx, candidate.Work.Href, attemptedAt, runID)
	dbWriteMu.Unlock()
	if err != nil {
		failure := detailFailuref(detailErrorTypeState, detailErrorStageMarkRunning, err, "mark detail running failed: href=%s", candidate.Work.Href)
		snapshot := state.recordFailure(failure)
		log.Printf("detail failed: run_id=%s %s error_type=%s error_stage=%s error=%v", runID, workLabel, detailErrorTypeState, detailErrorStageMarkRunning, failure)
		logDetailProgress(runID, snapshot)
		return
	}

	detail, err := fetcher.Fetch(ctx, candidate.Work)
	if err != nil {
		failure := ensureDetailFailure(err, detailErrorTypeNetwork, detailErrorStageRequest)
		dbWriteMu.Lock()
		markErr := repo.MarkDetailFailed(ctx, candidate.Work.Href, attemptedAt, runID, storage.DetailFetchFailure{
			Message:    failure.Error(),
			ErrorType:  failure.Type,
			ErrorStage: failure.Stage,
		})
		dbWriteMu.Unlock()
		if markErr != nil {
			log.Printf("detail state update failed: run_id=%s %s error=%v", runID, workLabel, markErr)
		}
		snapshot := state.recordFailure(failure)
		log.Printf("detail failed: run_id=%s %s error_type=%s error_stage=%s error=%v", runID, workLabel, failure.Type, failure.Stage, failure)
		logDetailProgress(runID, snapshot)
		return
	}

	dbWriteMu.Lock()
	saveErr := repo.SaveWorkDetail(ctx, detail, attemptedAt, runID)
	if saveErr != nil {
		markErr := repo.MarkDetailFailed(ctx, candidate.Work.Href, attemptedAt, runID, storage.DetailFetchFailure{
			Message:    saveErr.Error(),
			ErrorType:  detailErrorTypeWrite,
			ErrorStage: detailErrorStageSave,
		})
		dbWriteMu.Unlock()
		if markErr != nil {
			log.Printf("detail state update failed: run_id=%s %s error=%v", runID, workLabel, markErr)
		}
		failure := detailFailuref(detailErrorTypeWrite, detailErrorStageSave, saveErr, "save detail failed: href=%s", candidate.Work.Href)
		snapshot := state.recordFailure(failure)
		log.Printf("detail failed: run_id=%s %s error_type=%s error_stage=%s error=%v", runID, workLabel, failure.Type, failure.Stage, failure)
		logDetailProgress(runID, snapshot)
		return
	}
	dbWriteMu.Unlock()

	snapshot := state.recordSuccess()
	log.Printf("detail succeeded: run_id=%s %s title=%q", runID, workLabel, detail.Title)
	logDetailProgress(runID, snapshot)
}

func (s *DetailService) buildDetailFetchers(workerCount int) ([]detailFetcher, error) {
	proxyRotator, err := crawler.NewProxyRotator(s.cfg.ProxyURLs)
	if err != nil {
		return nil, detailFailuref(detailErrorTypeState, detailErrorStageRequest, err, "create detail proxy rotator failed")
	}

	transport := crawler.NewHTTPTransport(s.cfg.Debug, proxyRotator)
	source := s.normalizedSource()
	var allowedDomains []string
	if source == config.CrawlSourceHTML || source == config.CrawlSourceAuto {
		allowedDomains, err = allowedDomainsForBaseURL(s.cfg.BaseURL)
		if err != nil {
			return nil, detailFailuref(detailErrorTypeState, detailErrorStageRequest, err, "parse base url failed")
		}
	}

	fetchers := make([]detailFetcher, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		switch source {
		case config.CrawlSourceHTML:
			fetcher, err := newDetailWorkerFetcher(detailWorkerFetcherConfig{
				serviceConfig:   s.cfg,
				allowedDomains:  allowedDomains,
				sharedTransport: transport,
				sleep:           s.sleep,
			})
			if err != nil {
				return nil, err
			}
			fetchers = append(fetchers, fetcher)
		case config.CrawlSourceAPI, config.CrawlSourceAuto:
			var fallback detailFetcher
			if source == config.CrawlSourceAuto {
				htmlFetcher, err := newDetailWorkerFetcher(detailWorkerFetcherConfig{
					serviceConfig:   s.cfg,
					allowedDomains:  allowedDomains,
					sharedTransport: transport,
					sleep:           s.sleep,
				})
				if err != nil {
					return nil, err
				}
				fallback = htmlFetcher
			}

			fetchers = append(fetchers, &detailAPIFetcher{
				debug: s.cfg.Debug,
				api: detailapi.NewComposerAPI(
					s.backendBaseURL(),
					transport,
					30*time.Second,
					s.cfg.MaxRetries,
				),
				enricher: newDetailHTMLEnricher(s.cfg.Debug, transport),
				fallback: fallback,
			})
		default:
			return nil, detailFailuref(detailErrorTypeState, detailErrorStageRequest, nil, "unsupported detail source %q", source)
		}
	}
	return fetchers, nil
}

type detailFetcher interface {
	Fetch(context.Context, domain.Work) (domain.WorkDetail, error)
}

type detailAPIFetcher struct {
	debug    bool
	api      *detailapi.ComposerAPI
	enricher *detailHTMLEnricher
	fallback detailFetcher
}

func (f *detailAPIFetcher) Fetch(ctx context.Context, work domain.Work) (domain.WorkDetail, error) {
	detail, err := f.api.Fetch(ctx, work)
	if err != nil {
		if f.fallback != nil {
			return f.fallback.Fetch(ctx, work)
		}
		return domain.WorkDetail{}, detailFailuref(detailErrorTypeParse, detailErrorStageParse, err, "detail composer api fetch failed: href=%s", work.Href)
	}

	if f.enricher != nil {
		if enrichErr := f.enricher.Enrich(ctx, work, &detail); enrichErr != nil {
			log.Printf(
				"detail enrich warning: category=%s href=%s error=%v",
				work.Category,
				work.Href,
				enrichErr,
			)
		}
	}

	return detail, nil
}

type detailHTMLEnricher struct {
	debug  bool
	client *http.Client
}

func newDetailHTMLEnricher(debug bool, transport *http.Transport) *detailHTMLEnricher {
	var roundTripper http.RoundTripper
	if transport != nil {
		roundTripper = transport
	}
	return &detailHTMLEnricher{
		debug: debug,
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: roundTripper,
		},
	}
}

func (e *detailHTMLEnricher) Enrich(ctx context.Context, work domain.Work, detail *domain.WorkDetail) error {
	if strings.TrimSpace(work.Href) == "" || detail == nil {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, work.Href, nil)
	if err != nil {
		return err
	}
	setDetailHTTPHeaders(req)
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("detail enrich request failed: status=%d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return metacritic.EnrichDetail(work.Category, work.Href, bytes.NewReader(body), detail)
}

func setDetailHTTPHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Referer", "https://www.metacritic.com/")
}

type detailRunScope struct {
	Category  string
	TaskName  string
	FilterKey string
	Label     string
}

func buildDetailRunScope(task domain.DetailTask) detailRunScope {
	category := strings.TrimSpace(string(task.Category))
	if category == "" {
		category = "all"
	}

	taskName := "detail-" + category
	if strings.TrimSpace(task.WorkHref) != "" {
		taskName = "detail-single"
	}

	href := strings.TrimSpace(task.WorkHref)
	if href == "" {
		href = "all"
	}

	limit := "all"
	if task.Limit > 0 {
		limit = strconv.Itoa(task.Limit)
	}

	force := "0"
	if task.Force {
		force = "1"
	}

	filterKey := fmt.Sprintf("href=%s|force=%s|limit=%s", href, force, limit)
	label := fmt.Sprintf("category=%s href=%s force=%s limit=%s", category, href, force, limit)
	return detailRunScope{
		Category:  category,
		TaskName:  taskName,
		FilterKey: filterKey,
		Label:     label,
	}
}

func detailWorkLabel(work domain.Work) string {
	if strings.TrimSpace(work.Name) != "" {
		return fmt.Sprintf("category=%s name=%q href=%s", work.Category, work.Name, work.Href)
	}
	return fmt.Sprintf("category=%s href=%s", work.Category, work.Href)
}

func logDetailProgress(runID string, result DetailRunResult) {
	percent := 0
	if result.Total > 0 {
		percent = int(float64(result.Processed) / float64(result.Total) * 100)
	}
	log.Printf(
		"crawl detail progress: run_id=%s processed=%d/%d fetched=%d skipped=%d failed=%d recovered_running=%d percent=%d",
		runID,
		result.Processed,
		result.Total,
		result.Fetched,
		result.Skipped,
		result.Failed,
		result.RecoveredRunning,
		percent,
	)
}

func isFreshRunningCandidate(candidate storage.DetailCandidate, staleBefore time.Time) (bool, error) {
	if candidate.FetchStatus != storage.DetailFetchStatusRunning {
		return false, nil
	}
	lastAttempted := strings.TrimSpace(candidate.LastAttemptedAt)
	if lastAttempted == "" {
		return false, nil
	}

	attemptedAt, err := time.Parse(time.RFC3339, lastAttempted)
	if err != nil {
		return false, fmt.Errorf("invalid last_attempted_at %q: %w", candidate.LastAttemptedAt, err)
	}
	return attemptedAt.After(staleBefore), nil
}

type detailRetryPlan struct {
	Type       string
	StatusCode int
	Retriable  bool
}

func classifyDetailRequestFailure(r *colly.Response, err error) detailRetryPlan {
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return detailRetryPlan{Type: detailErrorTypeContext, Retriable: false}
	}

	statusCode := 0
	if r != nil {
		statusCode = r.StatusCode
	}

	switch {
	case statusCode == 403:
		return detailRetryPlan{Type: detailErrorTypeHTTP403, StatusCode: statusCode, Retriable: false}
	case statusCode == 404:
		return detailRetryPlan{Type: detailErrorTypeHTTP404, StatusCode: statusCode, Retriable: false}
	case statusCode == 429:
		return detailRetryPlan{Type: detailErrorTypeHTTP429, StatusCode: statusCode, Retriable: true}
	case statusCode >= 500 && statusCode <= 599:
		return detailRetryPlan{Type: detailErrorTypeHTTP5xx, StatusCode: statusCode, Retriable: true}
	case statusCode >= 400:
		return detailRetryPlan{Type: detailErrorTypeNetwork, StatusCode: statusCode, Retriable: false}
	default:
		return detailRetryPlan{Type: detailErrorTypeNetwork, StatusCode: statusCode, Retriable: true}
	}
}

func classifyDetailStatusCode(statusCode int) detailRetryPlan {
	switch {
	case statusCode == 403:
		return detailRetryPlan{Type: detailErrorTypeHTTP403, StatusCode: statusCode, Retriable: false}
	case statusCode == 404:
		return detailRetryPlan{Type: detailErrorTypeHTTP404, StatusCode: statusCode, Retriable: false}
	case statusCode == 429:
		return detailRetryPlan{Type: detailErrorTypeHTTP429, StatusCode: statusCode, Retriable: true}
	case statusCode >= 500 && statusCode <= 599:
		return detailRetryPlan{Type: detailErrorTypeHTTP5xx, StatusCode: statusCode, Retriable: true}
	case statusCode >= 400:
		return detailRetryPlan{Type: detailErrorTypeNetwork, StatusCode: statusCode, Retriable: false}
	default:
		return detailRetryPlan{Type: detailErrorTypeNetwork, StatusCode: statusCode, Retriable: true}
	}
}

func detailRetryDelay(errorType string, attempt int) time.Duration {
	switch errorType {
	case detailErrorTypeHTTP429:
		switch attempt {
		case 1:
			return 3 * time.Second
		case 2:
			return 6 * time.Second
		default:
			return 12 * time.Second
		}
	case detailErrorTypeHTTP5xx, detailErrorTypeNetwork:
		switch attempt {
		case 1:
			return 1 * time.Second
		case 2:
			return 2 * time.Second
		default:
			return 4 * time.Second
		}
	default:
		return time.Second
	}
}

func classifyDetailVisitError(err error, visitURL string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return detailFailuref(detailErrorTypeContext, detailErrorStageContext, err, "visit detail canceled: href=%s", visitURL)
	}
	return detailFailuref(detailErrorTypeNetwork, detailErrorStageRequest, err, "visit detail failed: href=%s", visitURL)
}

type detailFailure struct {
	Type    string
	Stage   string
	Message string
	Err     error
}

func (e *detailFailure) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %v", e.Message, e.Err)
}

func (e *detailFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func detailFailuref(errorType string, stage string, err error, format string, args ...interface{}) *detailFailure {
	return &detailFailure{
		Type:    errorType,
		Stage:   stage,
		Message: fmt.Sprintf(format, args...),
		Err:     err,
	}
}

func ensureDetailFailure(err error, defaultType string, defaultStage string) *detailFailure {
	if err == nil {
		return nil
	}
	var detailErr *detailFailure
	if errors.As(err, &detailErr) {
		return detailErr
	}
	return detailFailuref(defaultType, defaultStage, err, "detail operation failed")
}

type detailRunState struct {
	mu       sync.Mutex
	result   DetailRunResult
	firstErr error
}

func (s *detailRunState) recordSkip() DetailRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.Processed++
	s.result.Skipped++
	return s.result
}

func (s *detailRunState) recordSuccess() DetailRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.Processed++
	s.result.Fetched++
	s.result.DetailsUpserted++
	return s.result
}

func (s *detailRunState) recordFailure(err error) DetailRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.Processed++
	s.result.Failed++
	s.result.Failures++
	if err != nil && s.firstErr == nil {
		s.firstErr = err
	}
	return s.result
}

func (s *detailRunState) setFirstErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstErr == nil {
		s.firstErr = err
	}
}

func (s *detailRunState) snapshot() DetailRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (s *detailRunState) firstError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

type detailWorkerFetcherConfig struct {
	serviceConfig   DetailServiceConfig
	allowedDomains  []string
	sharedTransport *http.Transport
	sleep           func(time.Duration)
}

type detailFetchSession struct {
	ctx      context.Context
	work     domain.Work
	detail   domain.WorkDetail
	firstErr error
}

type detailWorkerFetcher struct {
	cfg          DetailServiceConfig
	sleep        func(time.Duration)
	collector    *colly.Collector
	retryTracker *crawler.RetryTracker
	mu           sync.Mutex
	session      *detailFetchSession
}

func newDetailWorkerFetcher(cfg detailWorkerFetcherConfig) (*detailWorkerFetcher, error) {
	collector, retryTracker, err := crawler.NewCollector(crawler.Config{
		AllowedDomains: cfg.allowedDomains,
		Debug:          cfg.serviceConfig.Debug,
		MaxRetries:     cfg.serviceConfig.MaxRetries,
		Transport:      cfg.sharedTransport,
	})
	if err != nil {
		return nil, detailFailuref(detailErrorTypeState, detailErrorStageRequest, err, "create detail collector failed")
	}

	fetcher := &detailWorkerFetcher{
		cfg:          cfg.serviceConfig,
		sleep:        cfg.sleep,
		collector:    collector,
		retryTracker: retryTracker,
	}
	fetcher.collector.ParseHTTPErrorResponse = true
	fetcher.collector.OnRequest(fetcher.onRequest)
	fetcher.collector.OnResponse(fetcher.onResponse)
	fetcher.collector.OnError(fetcher.onError)
	return fetcher, nil
}

func (f *detailWorkerFetcher) Fetch(ctx context.Context, work domain.Work) (domain.WorkDetail, error) {
	if err := ctx.Err(); err != nil {
		return domain.WorkDetail{}, detailFailuref(detailErrorTypeContext, detailErrorStageContext, err, "detail context canceled before request: href=%s", work.Href)
	}

	visitURL := strings.TrimSpace(work.Href)
	if visitURL == "" {
		return domain.WorkDetail{}, detailFailuref(detailErrorTypeState, detailErrorStageRequest, nil, "detail state invalid: href is empty")
	}

	f.retryTracker.Reset(visitURL)
	f.setSession(&detailFetchSession{ctx: ctx, work: work})
	defer f.clearSession()

	if err := f.collector.Visit(visitURL); err != nil {
		return domain.WorkDetail{}, classifyDetailVisitError(err, visitURL)
	}
	f.collector.Wait()

	session := f.snapshotSession()
	if session == nil {
		return domain.WorkDetail{}, detailFailuref(detailErrorTypeState, detailErrorStageRequest, nil, "detail request lost session: href=%s", work.Href)
	}
	if session.firstErr != nil {
		f.retryTracker.Reset(visitURL)
		return domain.WorkDetail{}, session.firstErr
	}
	if session.detail.Title == "" {
		return domain.WorkDetail{}, detailFailuref(detailErrorTypeParse, detailErrorStageParse, nil, "detail parse failed: href=%s category=%s field=title reason=empty_detail", work.Href, work.Category)
	}
	return session.detail, nil
}

func (f *detailWorkerFetcher) onRequest(r *colly.Request) {
	crawler.SetDefaultRequestHeaders(r)
	if f.cfg.Debug {
		log.Printf("visit detail: %s", r.URL.String())
	}
}

func (f *detailWorkerFetcher) onResponse(r *colly.Response) {
	session := f.snapshotSession()
	if session == nil {
		return
	}
	if err := session.ctx.Err(); err != nil {
		f.setFirstErr(detailFailuref(detailErrorTypeContext, detailErrorStageContext, err, "detail context canceled during response: href=%s", session.work.Href))
		return
	}
	if r.StatusCode >= 400 {
		if err := f.handleHTTPResponseFailure(r); err != nil {
			f.setFirstErr(err)
		}
		return
	}

	parsed, err := metacritic.ParseDetail(session.work.Category, session.work.Href, bytes.NewReader(r.Body))
	if err != nil {
		f.setFirstErr(detailFailuref(detailErrorTypeParse, detailErrorStageParse, err, "detail parse failed: href=%s", session.work.Href))
		return
	}
	f.retryTracker.Reset(r.Request.URL.String())
	f.setParsedDetail(parsed)
}

func (f *detailWorkerFetcher) onError(r *colly.Response, err error) {
	session := f.snapshotSession()
	if session == nil {
		return
	}
	if ctxErr := session.ctx.Err(); ctxErr != nil {
		f.setFirstErr(detailFailuref(detailErrorTypeContext, detailErrorStageContext, ctxErr, "detail context canceled during request: href=%s", session.work.Href))
		return
	}
	if r != nil && r.StatusCode >= 400 {
		return
	}

	retryPlan := classifyDetailRequestFailure(r, err)
	urlStr := session.work.Href
	if r != nil && r.Request != nil && r.Request.URL != nil {
		urlStr = r.Request.URL.String()
	}

	if !retryPlan.Retriable || r == nil || r.Request == nil {
		f.setFirstErr(detailFailuref(retryPlan.Type, detailErrorStageRequest, err, "detail request failed permanently: href=%s status=%d", urlStr, retryPlan.StatusCode))
		return
	}

	attempt, shouldRetry := f.retryTracker.Next(urlStr)
	if !shouldRetry {
		f.setFirstErr(detailFailuref(retryPlan.Type, detailErrorStageRequest, err, "detail request failed after retries: href=%s status=%d", urlStr, retryPlan.StatusCode))
		return
	}

	delay := detailRetryDelay(retryPlan.Type, attempt)
	if f.cfg.Debug {
		log.Printf("detail request retry: href=%s status=%d error_type=%s retry=%d/%d delay=%s err=%v", urlStr, retryPlan.StatusCode, retryPlan.Type, attempt, f.cfg.MaxRetries, delay, err)
	}
	f.sleep(delay)
	if retryErr := r.Request.Retry(); retryErr != nil {
		f.setFirstErr(detailFailuref(retryPlan.Type, detailErrorStageRequest, retryErr, "retry detail request failed: href=%s status=%d", urlStr, retryPlan.StatusCode))
	}
}

func (f *detailWorkerFetcher) handleHTTPResponseFailure(r *colly.Response) error {
	if r == nil || r.Request == nil || r.Request.URL == nil {
		return detailFailuref(detailErrorTypeNetwork, detailErrorStageRequest, nil, "detail request failed permanently: href=%s status=%d", "", 0)
	}

	retryPlan := classifyDetailStatusCode(r.StatusCode)
	urlStr := r.Request.URL.String()
	if !retryPlan.Retriable {
		return detailFailuref(retryPlan.Type, detailErrorStageRequest, nil, "detail request failed permanently: href=%s status=%d", urlStr, retryPlan.StatusCode)
	}

	attempt, shouldRetry := f.retryTracker.Next(urlStr)
	if !shouldRetry {
		return detailFailuref(retryPlan.Type, detailErrorStageRequest, nil, "detail request failed after retries: href=%s status=%d", urlStr, retryPlan.StatusCode)
	}

	delay := detailRetryDelay(retryPlan.Type, attempt)
	if f.cfg.Debug {
		log.Printf("detail request retry: href=%s status=%d error_type=%s retry=%d/%d delay=%s", urlStr, retryPlan.StatusCode, retryPlan.Type, attempt, f.cfg.MaxRetries, delay)
	}
	f.sleep(delay)
	if retryErr := r.Request.Retry(); retryErr != nil {
		return detailFailuref(retryPlan.Type, detailErrorStageRequest, retryErr, "retry detail request failed: href=%s status=%d", urlStr, retryPlan.StatusCode)
	}
	return nil
}

func (f *detailWorkerFetcher) setSession(session *detailFetchSession) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.session = session
}

func (f *detailWorkerFetcher) clearSession() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.session = nil
}

func (f *detailWorkerFetcher) snapshotSession() *detailFetchSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.session == nil {
		return nil
	}
	session := *f.session
	return &session
}

func (f *detailWorkerFetcher) setFirstErr(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.session != nil && f.session.firstErr == nil {
		f.session.firstErr = err
	}
}

func (f *detailWorkerFetcher) setParsedDetail(detail domain.WorkDetail) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.session != nil {
		f.session.detail = detail
	}
}
