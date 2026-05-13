package app

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/crawler"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	reviewapi "github.com/gofurry/metacritic-harvester/internal/source/metacritic/api"
	"github.com/gofurry/metacritic-harvester/internal/storage"
)

const reviewStaleRunningThreshold = 15 * time.Minute

const (
	reviewErrorTypeState   = "state"
	reviewErrorTypeRecover = "state_recovered"
	reviewErrorTypeContext = "context"
	reviewErrorTypeNetwork = "network"
	reviewErrorTypeHTTP403 = "http_403"
	reviewErrorTypeHTTP404 = "http_404"
	reviewErrorTypeHTTP429 = "http_429"
	reviewErrorTypeHTTP5xx = "http_5xx"
	reviewErrorTypeParse   = "parse"
	reviewErrorTypeWrite   = "write"
)

const (
	reviewErrorStagePlan     = "plan"
	reviewErrorStageRecovery = "recovery"
	reviewErrorStageRequest  = "request"
	reviewErrorStageMarkRun  = "mark_running"
	reviewErrorStagePage     = "page"
	reviewErrorStageSave     = "save"
	reviewErrorStageState    = "state"
	reviewErrorStageContext  = "context"
	reviewErrorStageValidate = "validate"
)

type ReviewServiceConfig struct {
	BaseURL         string
	RuntimePolicy   *crawler.HTTPRuntimePolicy
	DBPath          string
	Debug           bool
	ContinueOnError bool
	MaxRetries      int
	ProxyURLs       []string
}

type ReviewRunResult struct {
	RunID                 string
	RequestedSource       string
	EffectiveSource       string
	FallbackUsed          bool
	FallbackReason        string
	Candidates            int
	ScopesScheduled       int
	ScopesProcessed       int
	ScopesFetched         int
	ScopesSkipped         int
	ScopesFailed          int
	ReviewsFetched        int
	ReviewSnapshotsSaved  int
	LatestReviewsUpserted int
	Failures              int
}

type ReviewService struct {
	cfg   ReviewServiceConfig
	now   func() time.Time
	sleep func(time.Duration)
}

func NewReviewService(cfg ReviewServiceConfig) *ReviewService {
	return &ReviewService{
		cfg:   cfg,
		now:   time.Now,
		sleep: time.Sleep,
	}
}

func (s *ReviewService) Run(ctx context.Context, task domain.ReviewTask) (ReviewRunResult, error) {
	db, err := storage.Open(ctx, s.cfg.DBPath)
	if err != nil {
		return ReviewRunResult{}, err
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	runID := uuid.NewString()
	scope := buildReviewRunScope(task)
	startedAt := s.now().UTC()
	if err := repo.CreateReviewCrawlRun(ctx, runID, "crawl reviews", scope.TaskName, scope.Category, scope.FilterKey, startedAt); err != nil {
		return ReviewRunResult{}, err
	}

	result := ReviewRunResult{RunID: runID}
	result.RequestedSource = string(config.CrawlSourceAPI)
	result.EffectiveSource = string(config.CrawlSourceAPI)
	var finalErr error
	defer func() {
		finishedAt := s.now().UTC()
		if finalErr != nil {
			_ = repo.FailReviewCrawlRun(context.Background(), runID, finishedAt, finalErr.Error())
			return
		}
		_ = repo.CompleteReviewCrawlRun(context.Background(), runID, finishedAt)
	}()

	log.Printf(
		"crawl reviews start: run_id=%s requested_source=api effective_source=api category=%s work_href=%s review_type=%s sentiment=%s sort=%s platform=%s limit=%d page_size=%d max_pages=%d force=%t concurrency=%d db=%s",
		runID,
		scope.Category,
		task.WorkHref,
		task.ReviewType,
		task.Sentiment,
		task.Sort,
		task.Platform,
		task.Limit,
		task.PageSize,
		task.MaxPages,
		task.Force,
		task.Concurrency,
		s.cfg.DBPath,
	)

	candidates, err := repo.ListReviewCandidates(ctx, storage.ListReviewCandidatesFilter{
		Category: string(task.Category),
		WorkHref: task.WorkHref,
		Limit:    task.Limit,
	})
	if err != nil {
		finalErr = reviewFailuref(reviewErrorTypeState, reviewErrorStagePlan, err, "list review candidates failed: run_id=%s", runID)
		return result, finalErr
	}
	result.Candidates = len(candidates)
	if len(candidates) == 0 {
		log.Printf("crawl reviews finished without candidates: run_id=%s", runID)
		return result, nil
	}
	workerCount := task.Concurrency
	if workerCount <= 0 {
		workerCount = 1
	}

	proxyRotator, err := crawler.NewProxyRotator(s.cfg.ProxyURLs)
	if err != nil {
		finalErr = reviewFailuref(reviewErrorTypeState, reviewErrorStageRequest, err, "create review proxy rotator failed: run_id=%s", runID)
		return result, finalErr
	}
	policy := s.runtimePolicy(workerCount)
	transport := crawler.WrapTransportWithPolicy(crawler.NewHTTPTransport(s.cfg.Debug, proxyRotator), policy)
	pageAPI := reviewapi.NewReviewPageAPI(s.cfg.BaseURL, transport, policy.Timeout, s.cfg.MaxRetries)
	listAPI := reviewapi.NewReviewListAPI(s.cfg.BaseURL, transport, policy.Timeout, s.cfg.MaxRetries)

	state := &reviewRunState{
		result: ReviewRunResult{
			RunID:           runID,
			RequestedSource: string(config.CrawlSourceAPI),
			EffectiveSource: string(config.CrawlSourceAPI),
			Candidates:      len(candidates),
		},
	}

	scopeTasks, err := s.planReviewScopes(ctx, pageAPI, candidates, task, runID, state)
	if err != nil {
		finalErr = err
		return result, finalErr
	}
	state.addScopesScheduled(len(scopeTasks))
	result.ScopesScheduled = len(scopeTasks)
	if len(scopeTasks) == 0 {
		result = state.snapshot()
		if err := ctx.Err(); err != nil {
			finalErr = reviewFailuref(reviewErrorTypeContext, reviewErrorStageContext, err, "review crawl canceled: run_id=%s", runID)
			return result, finalErr
		}
		if err := state.firstError(); err != nil && !s.cfg.ContinueOnError {
			finalErr = fmt.Errorf("review crawl run %s completed with %d failure(s): %w", runID, result.Failures, err)
			return result, finalErr
		}
		log.Printf("crawl reviews finished without scopes: run_id=%s", runID)
		return result, nil
	}

	type reviewJob struct {
		index int
		task  reviewScopeTask
	}

	jobs := make(chan reviewJob)
	var wg sync.WaitGroup
	var dbWriteMu sync.Mutex

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				s.processReviewScope(ctx, repo, listAPI, job.task, runID, state, &dbWriteMu)
			}
		}()
	}

sendJobs:
	for idx, scopeTask := range scopeTasks {
		select {
		case <-ctx.Done():
			state.setFirstErr(reviewFailuref(reviewErrorTypeContext, reviewErrorStageContext, ctx.Err(), "review crawl canceled: run_id=%s", runID))
			break sendJobs
		case jobs <- reviewJob{index: idx, task: scopeTask}:
		}
	}
	close(jobs)
	wg.Wait()

	result = state.snapshot()
	if err := ctx.Err(); err != nil {
		finalErr = reviewFailuref(reviewErrorTypeContext, reviewErrorStageContext, err, "review crawl canceled: run_id=%s", runID)
		return result, finalErr
	}
	if err := state.firstError(); err != nil {
		if s.cfg.ContinueOnError {
			log.Printf(
				"crawl reviews finished with ignored failures: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s candidates=%d scopes=%d fetched=%d skipped=%d failed=%d reviews=%d snapshots=%d latest=%d failures=%d db=%s",
				runID,
				result.RequestedSource,
				result.EffectiveSource,
				result.FallbackUsed,
				result.FallbackReason,
				result.Candidates,
				result.ScopesScheduled,
				result.ScopesFetched,
				result.ScopesSkipped,
				result.ScopesFailed,
				result.ReviewsFetched,
				result.ReviewSnapshotsSaved,
				result.LatestReviewsUpserted,
				result.Failures,
				s.cfg.DBPath,
			)
			return result, nil
		}
		finalErr = fmt.Errorf("review crawl run %s completed with %d failure(s): %w", runID, result.Failures, err)
		return result, finalErr
	}

	log.Printf(
		"crawl reviews finished successfully: run_id=%s requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s candidates=%d scopes=%d fetched=%d skipped=%d failed=%d reviews=%d snapshots=%d latest=%d failures=%d db=%s",
		runID,
		result.RequestedSource,
		result.EffectiveSource,
		result.FallbackUsed,
		result.FallbackReason,
		result.Candidates,
		result.ScopesScheduled,
		result.ScopesFetched,
		result.ScopesSkipped,
		result.ScopesFailed,
		result.ReviewsFetched,
		result.ReviewSnapshotsSaved,
		result.LatestReviewsUpserted,
		result.Failures,
		s.cfg.DBPath,
	)
	return result, nil
}

func (s *ReviewService) runtimePolicy(concurrency int) crawler.HTTPRuntimePolicy {
	return applyRuntimePolicyOverride(reviewRuntimePolicy(concurrency), s.cfg.RuntimePolicy)
}

type reviewScopeTask struct {
	Task        domain.ReviewTask
	Work        domain.Work
	ReviewType  domain.ReviewType
	PlatformKey string
	PageContext reviewapi.ReviewPageContext
}

func (s *ReviewService) planReviewScopes(ctx context.Context, pageAPI *reviewapi.ReviewPageAPI, candidates []domain.Work, task domain.ReviewTask, runID string, state *reviewRunState) ([]reviewScopeTask, error) {
	types := reviewTypesForTask(task.ReviewType)
	scopes := make([]reviewScopeTask, 0, len(candidates))

	for _, work := range candidates {
		if err := ctx.Err(); err != nil {
			return scopes, reviewFailuref(reviewErrorTypeContext, reviewErrorStageContext, err, "review crawl canceled: run_id=%s", runID)
		}
		pageContextType := task.ReviewType
		if pageContextType == domain.ReviewTypeAll {
			pageContextType = domain.ReviewTypeCritic
		}
		pageCtx, err := pageAPI.FetchContext(ctx, work, pageContextType)
		if err != nil {
			failureType := classifyReviewFetchError(err).Type
			failure := reviewFailuref(failureType, reviewErrorStagePlan, err, "fetch review page context failed: href=%s", work.Href)
			snapshot := state.recordPlannedFailure(failure)
			log.Printf("review planning failed: run_id=%s href=%s error_type=%s error_stage=%s error=%v", runID, work.Href, failure.Type, failure.Stage, failure)
			logReviewProgress(runID, snapshot)
			if !s.cfg.ContinueOnError {
				return scopes, failure
			}
			continue
		}

		platformKeys := reviewPlatformKeys(task, work, pageCtx)
		for _, reviewType := range types {
			for _, platformKey := range platformKeys {
				scopes = append(scopes, reviewScopeTask{
					Task:        task,
					Work:        work,
					ReviewType:  reviewType,
					PlatformKey: platformKey,
					PageContext: pageCtx,
				})
			}
		}
	}

	return scopes, nil
}

func reviewTypesForTask(reviewType domain.ReviewType) []domain.ReviewType {
	switch reviewType {
	case domain.ReviewTypeAll:
		return []domain.ReviewType{domain.ReviewTypeCritic, domain.ReviewTypeUser}
	case domain.ReviewTypeCritic, domain.ReviewTypeUser:
		return []domain.ReviewType{reviewType}
	default:
		return []domain.ReviewType{domain.ReviewTypeCritic}
	}
}

func reviewPlatformKeys(task domain.ReviewTask, work domain.Work, ctx reviewapi.ReviewPageContext) []string {
	if work.Category != domain.CategoryGame {
		return []string{""}
	}

	if strings.TrimSpace(task.Platform) != "" {
		return []string{strings.TrimSpace(task.Platform)}
	}

	keys := make([]string, 0, len(ctx.Platforms))
	for _, platform := range ctx.Platforms {
		if strings.TrimSpace(platform.Key) != "" {
			keys = append(keys, strings.TrimSpace(platform.Key))
		}
	}
	if len(keys) == 0 {
		return []string{""}
	}
	return keys
}

func (s *ReviewService) processReviewScope(
	ctx context.Context,
	repo *storage.Repository,
	listAPI *reviewapi.ReviewListAPI,
	scopeTask reviewScopeTask,
	runID string,
	state *reviewRunState,
	dbWriteMu *sync.Mutex,
) {
	scope := domain.ReviewScope{
		WorkHref:    scopeTask.Work.Href,
		Category:    scopeTask.Work.Category,
		ReviewType:  scopeTask.ReviewType,
		PlatformKey: scopeTask.PlatformKey,
	}
	scopeLabel := scope.Key()

	if err := ctx.Err(); err != nil {
		snapshot := state.recordFailure(reviewFailuref(reviewErrorTypeContext, reviewErrorStageContext, err, "review crawl canceled: run_id=%s scope=%s", runID, scopeLabel))
		logReviewProgress(runID, snapshot)
		return
	}

	if !s.prepareReviewScopeState(ctx, repo, scopeTask.Task, scope, runID, state, dbWriteMu) {
		return
	}

	attemptedAt := s.now().UTC()
	dbWriteMu.Lock()
	stateErr := repo.MarkReviewRunning(ctx, scope, attemptedAt, runID)
	dbWriteMu.Unlock()
	if stateErr != nil {
		snapshot := state.recordFailure(reviewFailuref(reviewErrorTypeState, reviewErrorStageMarkRun, stateErr, "mark review running failed: href=%s", scope.WorkHref))
		logReviewProgress(runID, snapshot)
		return
	}

	limit := s.defaultReviewPageSize(scopeTask.Task)
	offset := 0
	fetched := 0
	pageCount := 0

	for {
		if taskMaxPages := s.reviewMaxPages(scopeTask.Task); taskMaxPages > 0 && pageCount >= taskMaxPages {
			break
		}

		page, err := listAPI.FetchPage(ctx, scopeTask.Work, scopeTask.ReviewType, scopeTask.Task.Sentiment, scopeTask.Task.Sort, scopeTask.PlatformKey, offset, limit)
		if err != nil {
			failure := classifyReviewFetchError(err)
			dbWriteMu.Lock()
			markErr := repo.MarkReviewFailed(ctx, scope, attemptedAt, runID, storage.ReviewFetchFailure{
				Message:    failure.Error(),
				ErrorType:  failure.Type,
				ErrorStage: failure.Stage,
			})
			dbWriteMu.Unlock()
			if markErr != nil {
				log.Printf("review state update failed: run_id=%s scope=%s err=%v", runID, scopeLabel, markErr)
			}
			snapshot := state.recordFailure(failure)
			log.Printf("review failed: run_id=%s scope=%s error_type=%s error_stage=%s error=%v", runID, scopeLabel, failure.Type, failure.Stage, failure)
			logReviewProgress(runID, snapshot)
			return
		}

		if len(page.Items) == 0 {
			break
		}

		now := s.now().UTC()
		for i := range page.Items {
			page.Items[i].CrawlRunID = runID
			page.Items[i].Category = scopeTask.Work.Category
			page.Items[i].WorkHref = scopeTask.Work.Href
			page.Items[i].ReviewType = scopeTask.ReviewType
			if strings.TrimSpace(page.Items[i].PlatformKey) == "" {
				page.Items[i].PlatformKey = scopeTask.PlatformKey
			}
			page.Items[i].CrawledAt = now
		}

		dbWriteMu.Lock()
		saveErr := repo.SaveReviewRecords(ctx, page.Items)
		dbWriteMu.Unlock()
		if saveErr != nil {
			dbWriteMu.Lock()
			markErr := repo.MarkReviewFailed(ctx, scope, attemptedAt, runID, storage.ReviewFetchFailure{
				Message:    saveErr.Error(),
				ErrorType:  reviewErrorTypeWrite,
				ErrorStage: reviewErrorStageSave,
			})
			dbWriteMu.Unlock()
			if markErr != nil {
				log.Printf("review state update failed: run_id=%s scope=%s err=%v", runID, scopeLabel, markErr)
			}
			failure := reviewFailuref(reviewErrorTypeWrite, reviewErrorStageSave, saveErr, "save review page failed: href=%s", scope.WorkHref)
			snapshot := state.recordFailure(failure)
			log.Printf("review failed: run_id=%s scope=%s error_type=%s error_stage=%s error=%v", runID, scopeLabel, failure.Type, failure.Stage, failure)
			logReviewProgress(runID, snapshot)
			return
		}

		fetched += len(page.Items)
		resultSnapshot := state.recordPageSuccess(len(page.Items))
		logReviewProgress(runID, resultSnapshot)

		pageCount++
		offset += len(page.Items)
		if page.TotalResults > 0 && offset >= page.TotalResults {
			break
		}
	}

	fetchedAt := s.now().UTC()
	dbWriteMu.Lock()
	markErr := repo.MarkReviewSucceeded(ctx, scope, attemptedAt, fetchedAt, runID)
	dbWriteMu.Unlock()
	if markErr != nil {
		failure := reviewFailuref(reviewErrorTypeState, reviewErrorStageState, markErr, "mark review succeeded failed: href=%s", scope.WorkHref)
		snapshot := state.recordFailure(failure)
		log.Printf("review failed: run_id=%s scope=%s error_type=%s error_stage=%s error=%v", runID, scopeLabel, failure.Type, failure.Stage, failure)
		logReviewProgress(runID, snapshot)
		return
	}

	snapshot := state.recordScopeFetched()
	log.Printf("review succeeded: run_id=%s scope=%s fetched=%d", runID, scopeLabel, fetched)
	logReviewProgress(runID, snapshot)
}

func (s *ReviewService) prepareReviewScopeState(
	ctx context.Context,
	repo *storage.Repository,
	task domain.ReviewTask,
	scope domain.ReviewScope,
	runID string,
	state *reviewRunState,
	dbWriteMu *sync.Mutex,
) bool {
	scopeLabel := scope.Key()
	dbWriteMu.Lock()
	fetchState, err := repo.GetReviewFetchState(ctx, scope)
	dbWriteMu.Unlock()
	if err != nil && !storage.IsNotFound(err) {
		snapshot := state.recordFailure(reviewFailuref(reviewErrorTypeState, reviewErrorStageState, err, "load review fetch state failed: scope=%s", scopeLabel))
		logReviewProgress(runID, snapshot)
		return false
	}
	if storage.IsNotFound(err) {
		return true
	}

	if fetchState.Status == storage.ReviewFetchStatusSucceeded && !task.Force {
		snapshot := state.recordSkip()
		log.Printf("review skipped: run_id=%s scope=%s reason=already_succeeded", runID, scopeLabel)
		logReviewProgress(runID, snapshot)
		return false
	}

	if fetchState.Status != storage.ReviewFetchStatusRunning {
		return true
	}

	lastAttemptedAt, ok := parseRFC3339NullString(fetchState.LastAttemptedAt)
	if ok && lastAttemptedAt.After(s.now().UTC().Add(-reviewStaleRunningThreshold)) {
		snapshot := state.recordSkip()
		log.Printf("review skipped: run_id=%s scope=%s reason=fresh_running last_attempted_at=%s", runID, scopeLabel, lastAttemptedAt.Format(time.RFC3339))
		logReviewProgress(runID, snapshot)
		return false
	}

	dbWriteMu.Lock()
	recoverErr := repo.MarkReviewFailed(ctx, scope, s.now().UTC(), runID, storage.ReviewFetchFailure{
		Message:    "recovered stale running state",
		ErrorType:  reviewErrorTypeRecover,
		ErrorStage: reviewErrorStageRecovery,
	})
	dbWriteMu.Unlock()
	if recoverErr != nil {
		snapshot := state.recordFailure(reviewFailuref(reviewErrorTypeState, reviewErrorStageRecovery, recoverErr, "recover stale review state failed: scope=%s", scopeLabel))
		logReviewProgress(runID, snapshot)
		return false
	}
	log.Printf("review stale-running recovered: run_id=%s scope=%s", runID, scopeLabel)
	return true
}

func (s *ReviewService) defaultReviewPageSize(task domain.ReviewTask) int {
	if task.PageSize > 0 {
		return task.PageSize
	}
	return 20
}

func (s *ReviewService) reviewMaxPages(task domain.ReviewTask) int {
	if task.MaxPages > 0 {
		return task.MaxPages
	}
	return 0
}

type reviewRunScope struct {
	Category  string
	TaskName  string
	FilterKey string
}

// Reviews remain scope-recoverable instead of page-recoverable on purpose.
// The backend exposes an append-like paginated stream whose offsets can drift as
// new reviews arrive, so persisting per-page checkpoints would not provide a
// stable resume boundary. Re-running an entire scope is cheaper to reason
// about, and latest_reviews + review_snapshots already make scope replays
// idempotent.
func buildReviewRunScope(task domain.ReviewTask) reviewRunScope {
	category := strings.TrimSpace(string(task.Category))
	if category == "" {
		category = "all"
	}
	reviewType := strings.TrimSpace(string(task.ReviewType))
	if reviewType == "" {
		reviewType = string(domain.ReviewTypeAll)
	}
	workHref := strings.TrimSpace(task.WorkHref)
	if workHref == "" {
		workHref = "all"
	}
	platform := strings.TrimSpace(task.Platform)
	sentiment := strings.TrimSpace(string(task.Sentiment))
	if sentiment == "" {
		sentiment = string(domain.ReviewSentimentAll)
	}
	sort := strings.TrimSpace(string(task.Sort))
	limit := task.Limit
	pageSize := task.PageSize
	maxPages := task.MaxPages
	force := task.Force
	filterKey := fmt.Sprintf("href=%s|review_type=%s|platform=%s|sentiment=%s|sort=%s|limit=%d|page_size=%d|max_pages=%d|force=%t", workHref, reviewType, platform, sentiment, sort, limit, pageSize, maxPages, force)
	taskName := fmt.Sprintf("reviews-%s-%s", category, reviewType)
	if strings.TrimSpace(task.WorkHref) != "" {
		taskName = "reviews-single"
	}
	return reviewRunScope{
		Category:  category,
		TaskName:  taskName,
		FilterKey: filterKey,
	}
}

type reviewFailure struct {
	Type    string
	Stage   string
	Message string
	Err     error
}

func (e *reviewFailure) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %v", e.Message, e.Err)
}

func reviewFailuref(errorType string, stage string, err error, format string, args ...any) *reviewFailure {
	return &reviewFailure{
		Type:    errorType,
		Stage:   stage,
		Message: fmt.Sprintf(format, args...),
		Err:     err,
	}
}

func classifyReviewFetchError(err error) *reviewFailure {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "status=403"):
		return reviewFailuref(reviewErrorTypeHTTP403, reviewErrorStageRequest, err, "review request failed")
	case strings.Contains(msg, "status=404"):
		return reviewFailuref(reviewErrorTypeHTTP404, reviewErrorStageRequest, err, "review request failed")
	case strings.Contains(msg, "status=429"):
		return reviewFailuref(reviewErrorTypeHTTP429, reviewErrorStageRequest, err, "review request failed")
	case strings.Contains(msg, "status=5"):
		return reviewFailuref(reviewErrorTypeHTTP5xx, reviewErrorStageRequest, err, "review request failed")
	default:
		return reviewFailuref(reviewErrorTypeNetwork, reviewErrorStageRequest, err, "review request failed")
	}
}

func parseRFC3339NullString(value sql.NullString) (time.Time, bool) {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, value.String)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

type reviewRunState struct {
	mu       sync.Mutex
	result   ReviewRunResult
	firstErr error
}

func (s *reviewRunState) addScopesScheduled(count int) {
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.ScopesScheduled += count
}

func (s *reviewRunState) recordPageSuccess(reviewCount int) ReviewRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.ReviewsFetched += reviewCount
	s.result.ReviewSnapshotsSaved += reviewCount
	s.result.LatestReviewsUpserted += reviewCount
	return s.result
}

func (s *reviewRunState) recordScopeFetched() ReviewRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.ScopesProcessed++
	s.result.ScopesFetched++
	return s.result
}

func (s *reviewRunState) recordSkip() ReviewRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.ScopesProcessed++
	s.result.ScopesSkipped++
	return s.result
}

func (s *reviewRunState) recordPlannedFailure(err error) ReviewRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.ScopesScheduled++
	s.result.ScopesProcessed++
	s.result.ScopesFailed++
	s.result.Failures++
	if s.firstErr == nil && err != nil {
		s.firstErr = err
	}
	return s.result
}

func (s *reviewRunState) recordFailure(err error) ReviewRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.ScopesProcessed++
	s.result.ScopesFailed++
	s.result.Failures++
	if s.firstErr == nil && err != nil {
		s.firstErr = err
	}
	return s.result
}

func (s *reviewRunState) setFirstErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstErr == nil {
		s.firstErr = err
	}
}

func (s *reviewRunState) snapshot() ReviewRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (s *reviewRunState) firstError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

func logReviewProgress(runID string, result ReviewRunResult) {
	percent := 0
	if result.ScopesScheduled > 0 {
		percent = int(float64(result.ScopesProcessed) / float64(result.ScopesScheduled) * 100)
	}
	log.Printf(
		"crawl reviews progress: run_id=%s processed=%d/%d fetched=%d skipped=%d failed=%d reviews=%d snapshots=%d latest=%d failures=%d requested_source=%s effective_source=%s fallback_used=%t fallback_reason=%s percent=%d",
		runID,
		result.ScopesProcessed,
		result.ScopesScheduled,
		result.ScopesFetched,
		result.ScopesSkipped,
		result.ScopesFailed,
		result.ReviewsFetched,
		result.ReviewSnapshotsSaved,
		result.LatestReviewsUpserted,
		result.Failures,
		result.RequestedSource,
		result.EffectiveSource,
		result.FallbackUsed,
		result.FallbackReason,
		percent,
	)
}
