package app

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
)

type BatchTaskResult struct {
	Name                  string
	Kind                  string
	RunID                 string
	Category              string
	Metric                string
	DBPath                string
	Success               bool
	Error                 string
	PagesVisited          int
	PagesScheduled        int
	PagesSucceeded        int
	PagesWritten          int
	WorksUpserted         int
	ListEntriesInserted   int
	LatestEntriesUpserted int
	Processed             int
	Fetched               int
	Skipped               int
	RecoveredRunning      int
	DetailsUpserted       int
	ReviewScopesScheduled int
	ReviewScopesFetched   int
	ReviewScopesSkipped   int
	ReviewScopesFailed    int
	ReviewsFetched        int
	ReviewSnapshotsSaved  int
	ReviewLatestUpserted  int
	Failures              int
}

type BatchRunResult struct {
	Tasks                      []BatchTaskResult
	TotalTasks                 int
	SucceededTasks             int
	FailedTasks                int
	TotalPagesScheduled        int
	TotalPagesSucceeded        int
	TotalPagesWritten          int
	TotalWorksUpserted         int
	TotalListEntriesInserted   int
	TotalLatestEntriesUpserted int
	TotalDetailProcessed       int
	TotalDetailFetched         int
	TotalDetailSkipped         int
	TotalRecoveredRunning      int
	TotalDetailsUpserted       int
	TotalReviewScopesScheduled int
	TotalReviewScopesFetched   int
	TotalReviewScopesSkipped   int
	TotalReviewScopesFailed    int
	TotalReviewsFetched        int
	TotalReviewSnapshotsSaved  int
	TotalReviewLatestUpserted  int
	TotalFailures              int
}

type BatchService struct {
	baseURL          string
	reviewBaseURL    string
	newListService   func(ListServiceConfig) listTaskRunner
	newDetailService func(DetailServiceConfig) detailTaskRunner
	newReviewService func(ReviewServiceConfig) reviewTaskRunner
	dbLockSet        *dbLockSet
}

func NewBatchService(baseURL string) *BatchService {
	return &BatchService{
		baseURL:       baseURL,
		reviewBaseURL: config.DefaultBackendBaseURL,
		dbLockSet:     newDBLockSet(),
		newListService: func(cfg ListServiceConfig) listTaskRunner {
			return NewListService(cfg)
		},
		newDetailService: func(cfg DetailServiceConfig) detailTaskRunner {
			return NewDetailService(cfg)
		},
		newReviewService: func(cfg ReviewServiceConfig) reviewTaskRunner {
			return NewReviewService(cfg)
		},
	}
}

func (s *BatchService) Run(ctx context.Context, tasks []config.BatchTaskConfig) BatchRunResult {
	return s.RunWithConcurrency(ctx, tasks, 1)
}

func (s *BatchService) RunWithConcurrency(ctx context.Context, tasks []config.BatchTaskConfig, concurrency int) BatchRunResult {
	if concurrency <= 0 {
		concurrency = 1
	}

	result := BatchRunResult{
		Tasks:      make([]BatchTaskResult, len(tasks)),
		TotalTasks: len(tasks),
	}

	type batchJob struct {
		index int
		task  config.BatchTaskConfig
	}

	jobs := make(chan batchJob)
	var wg sync.WaitGroup

	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := ctx.Err(); err != nil {
					result.Tasks[job.index] = canceledBatchTaskResult(job.task, err)
					continue
				}
				unlock := s.dbLockSet.Lock(taskDBPath(job.task))
				result.Tasks[job.index] = s.runTask(ctx, job.task)
				unlock()
			}
		}()
	}

sendJobs:
	for idx, task := range tasks {
		select {
		case <-ctx.Done():
			result.Tasks[idx] = canceledBatchTaskResult(task, ctx.Err())
			for remaining := idx + 1; remaining < len(tasks); remaining++ {
				result.Tasks[remaining] = canceledBatchTaskResult(tasks[remaining], ctx.Err())
			}
			break sendJobs
		case jobs <- batchJob{index: idx, task: task}:
		}
	}
	close(jobs)
	wg.Wait()

	for _, taskResult := range result.Tasks {
		result.TotalPagesScheduled += taskResult.PagesScheduled
		result.TotalPagesSucceeded += taskResult.PagesSucceeded
		result.TotalPagesWritten += taskResult.PagesWritten
		result.TotalWorksUpserted += taskResult.WorksUpserted
		result.TotalListEntriesInserted += taskResult.ListEntriesInserted
		result.TotalLatestEntriesUpserted += taskResult.LatestEntriesUpserted
		result.TotalDetailProcessed += taskResult.Processed
		result.TotalDetailFetched += taskResult.Fetched
		result.TotalDetailSkipped += taskResult.Skipped
		result.TotalRecoveredRunning += taskResult.RecoveredRunning
		result.TotalDetailsUpserted += taskResult.DetailsUpserted
		result.TotalReviewScopesScheduled += taskResult.ReviewScopesScheduled
		result.TotalReviewScopesFetched += taskResult.ReviewScopesFetched
		result.TotalReviewScopesSkipped += taskResult.ReviewScopesSkipped
		result.TotalReviewScopesFailed += taskResult.ReviewScopesFailed
		result.TotalReviewsFetched += taskResult.ReviewsFetched
		result.TotalReviewSnapshotsSaved += taskResult.ReviewSnapshotsSaved
		result.TotalReviewLatestUpserted += taskResult.ReviewLatestUpserted
		result.TotalFailures += taskResult.Failures
		if taskResult.Success {
			result.SucceededTasks++
			continue
		}
		result.FailedTasks++
	}

	return result
}

func (s *BatchService) runTask(ctx context.Context, task config.BatchTaskConfig) BatchTaskResult {
	switch normalizedBatchTaskKind(task) {
	case config.BatchTaskKindDetail:
		return s.runDetailTask(ctx, task)
	case config.BatchTaskKindReview:
		return s.runReviewTask(ctx, task)
	default:
		return s.runListTask(ctx, task)
	}
}

func (s *BatchService) runListTask(ctx context.Context, task config.BatchTaskConfig) BatchTaskResult {
	listCfg := task.List
	listService := s.newListService(ListServiceConfig{
		BaseURL:        s.baseURL,
		BackendBaseURL: s.reviewBaseURL,
		Source:         listCfg.Source,
		DBPath:         listCfg.DBPath,
		Debug:          listCfg.Debug,
		MaxRetries:     listCfg.MaxRetries,
		ProxyURLs:      listCfg.ProxyURLs,
		RunSource:      "crawl batch",
		TaskName:       task.Name,
	})

	taskResult := BatchTaskResult{
		Name:     task.Name,
		Kind:     string(normalizedBatchTaskKind(task)),
		Category: string(listCfg.Task.Category),
		Metric:   string(listCfg.Task.Metric),
		DBPath:   listCfg.DBPath,
	}

	runResult, err := listService.Run(ctx, listCfg.Task)
	taskResult.RunID = runResult.RunID
	taskResult.PagesVisited = runResult.PagesVisited
	taskResult.PagesScheduled = runResult.PagesScheduled
	taskResult.PagesSucceeded = runResult.PagesSucceeded
	taskResult.PagesWritten = runResult.PagesWritten
	taskResult.WorksUpserted = runResult.WorksUpserted
	taskResult.ListEntriesInserted = runResult.ListEntriesInserted
	taskResult.LatestEntriesUpserted = runResult.LatestEntriesUpserted
	taskResult.Failures = runResult.Failures
	taskResult.Success = err == nil
	if err != nil {
		taskResult.Error = err.Error()
	}
	return taskResult
}

func (s *BatchService) runDetailTask(ctx context.Context, task config.BatchTaskConfig) BatchTaskResult {
	detailCfg := task.Detail
	detailService := s.newDetailService(DetailServiceConfig{
		BaseURL:        s.baseURL,
		BackendBaseURL: s.reviewBaseURL,
		Source:         detailCfg.Source,
		DBPath:         detailCfg.DBPath,
		Debug:          detailCfg.Debug,
		MaxRetries:     detailCfg.MaxRetries,
		ProxyURLs:      detailCfg.ProxyURLs,
	})

	taskResult := BatchTaskResult{
		Name:     task.Name,
		Kind:     string(normalizedBatchTaskKind(task)),
		Category: string(detailCfg.Task.Category),
		Metric:   "detail",
		DBPath:   detailCfg.DBPath,
	}

	runResult, err := detailService.Run(ctx, detailCfg.Task)
	taskResult.RunID = runResult.RunID
	taskResult.Processed = runResult.Processed
	taskResult.Fetched = runResult.Fetched
	taskResult.Skipped = runResult.Skipped
	taskResult.RecoveredRunning = runResult.RecoveredRunning
	taskResult.DetailsUpserted = runResult.DetailsUpserted
	taskResult.Failures = runResult.Failures
	taskResult.Success = err == nil
	if err != nil {
		taskResult.Error = err.Error()
	}
	return taskResult
}

func (s *BatchService) runReviewTask(ctx context.Context, task config.BatchTaskConfig) BatchTaskResult {
	reviewCfg := task.Review
	reviewService := s.newReviewService(ReviewServiceConfig{
		BaseURL:    s.reviewBaseURL,
		DBPath:     reviewCfg.DBPath,
		Debug:      reviewCfg.Debug,
		MaxRetries: reviewCfg.MaxRetries,
		ProxyURLs:  reviewCfg.ProxyURLs,
	})

	taskResult := BatchTaskResult{
		Name:     task.Name,
		Kind:     string(normalizedBatchTaskKind(task)),
		Category: string(reviewCfg.Task.Category),
		Metric:   "reviews",
		DBPath:   reviewCfg.DBPath,
	}

	runResult, err := reviewService.Run(ctx, reviewCfg.Task)
	taskResult.RunID = runResult.RunID
	taskResult.ReviewScopesScheduled = runResult.ScopesScheduled
	taskResult.ReviewScopesFetched = runResult.ScopesFetched
	taskResult.ReviewScopesSkipped = runResult.ScopesSkipped
	taskResult.ReviewScopesFailed = runResult.ScopesFailed
	taskResult.ReviewsFetched = runResult.ReviewsFetched
	taskResult.ReviewSnapshotsSaved = runResult.ReviewSnapshotsSaved
	taskResult.ReviewLatestUpserted = runResult.LatestReviewsUpserted
	taskResult.Failures = runResult.Failures
	taskResult.Success = err == nil
	if err != nil {
		taskResult.Error = err.Error()
	}
	return taskResult
}

func canceledBatchTaskResult(task config.BatchTaskConfig, err error) BatchTaskResult {
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	return BatchTaskResult{
		Name:     task.Name,
		Kind:     string(normalizedBatchTaskKind(task)),
		Category: taskCategory(task),
		Metric:   taskMetric(task),
		DBPath:   taskDBPath(task),
		Success:  false,
		Error:    errText,
	}
}

func (r BatchRunResult) Error() error {
	if r.FailedTasks == 0 {
		return nil
	}
	return fmt.Errorf("batch run completed with %d failed task(s)", r.FailedTasks)
}

type listTaskRunner interface {
	Run(context.Context, domain.ListTask) (ListRunResult, error)
}

type listTaskRunnerFunc func(context.Context, domain.ListTask) (ListRunResult, error)

func (f listTaskRunnerFunc) Run(ctx context.Context, task domain.ListTask) (ListRunResult, error) {
	return f(ctx, task)
}

type detailTaskRunner interface {
	Run(context.Context, domain.DetailTask) (DetailRunResult, error)
}

type detailTaskRunnerFunc func(context.Context, domain.DetailTask) (DetailRunResult, error)

func (f detailTaskRunnerFunc) Run(ctx context.Context, task domain.DetailTask) (DetailRunResult, error) {
	return f(ctx, task)
}

type dbLockSet struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newDBLockSet() *dbLockSet {
	return &dbLockSet{
		locks: make(map[string]*sync.Mutex),
	}
}

func (s *dbLockSet) Lock(dbPath string) func() {
	key := filepath.Clean(dbPath)
	s.mu.Lock()
	lock, ok := s.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		s.locks[key] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	return func() {
		lock.Unlock()
	}
}

func taskDBPath(task config.BatchTaskConfig) string {
	if normalizedBatchTaskKind(task) == config.BatchTaskKindDetail && task.Detail != nil {
		return task.Detail.DBPath
	}
	if task.List != nil {
		return task.List.DBPath
	}
	return ""
}

func taskCategory(task config.BatchTaskConfig) string {
	if normalizedBatchTaskKind(task) == config.BatchTaskKindDetail && task.Detail != nil {
		return string(task.Detail.Task.Category)
	}
	if task.List != nil {
		return string(task.List.Task.Category)
	}
	return ""
}

func taskMetric(task config.BatchTaskConfig) string {
	if normalizedBatchTaskKind(task) == config.BatchTaskKindDetail {
		return "detail"
	}
	if normalizedBatchTaskKind(task) == config.BatchTaskKindReview {
		return "reviews"
	}
	if task.List != nil {
		return string(task.List.Task.Metric)
	}
	return ""
}

func normalizedBatchTaskKind(task config.BatchTaskConfig) config.BatchTaskKind {
	if task.Kind == config.BatchTaskKindDetail {
		return config.BatchTaskKindDetail
	}
	if task.Kind == config.BatchTaskKindReview {
		return config.BatchTaskKindReview
	}
	return config.BatchTaskKindList
}

type reviewTaskRunner interface {
	Run(context.Context, domain.ReviewTask) (ReviewRunResult, error)
}

type reviewTaskRunnerFunc func(context.Context, domain.ReviewTask) (ReviewRunResult, error)

func (f reviewTaskRunnerFunc) Run(ctx context.Context, task domain.ReviewTask) (ReviewRunResult, error) {
	return f(ctx, task)
}
