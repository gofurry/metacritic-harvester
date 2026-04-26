package serve

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GoFurry/metacritic-harvester/internal/app"
	"github.com/GoFurry/metacritic-harvester/internal/config"
	"github.com/GoFurry/metacritic-harvester/internal/domain"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusSucceeded TaskStatus = "succeeded"
	TaskStatusFailed    TaskStatus = "failed"
)

type TaskView struct {
	ID              string         `json:"id"`
	Kind            string         `json:"kind"`
	Status          TaskStatus     `json:"status"`
	CreatedAt       string         `json:"created_at"`
	StartedAt       string         `json:"started_at,omitempty"`
	FinishedAt      string         `json:"finished_at,omitempty"`
	Error           string         `json:"error,omitempty"`
	RunID           string         `json:"run_id,omitempty"`
	RequestedSource string         `json:"requested_source,omitempty"`
	EffectiveSource string         `json:"effective_source,omitempty"`
	FallbackUsed    bool           `json:"fallback_used"`
	FallbackReason  string         `json:"fallback_reason,omitempty"`
	Outcome         map[string]any `json:"outcome,omitempty"`
}

type taskDispatcher interface {
	List() []TaskView
	Get(id string) (TaskView, bool)
	SubmitList(config.ListCommandConfig) (TaskView, error)
	SubmitDetail(config.DetailCommandConfig) (TaskView, error)
	SubmitReview(config.ReviewCommandConfig) (TaskView, error)
}

type TaskManager struct {
	cfg              Config
	rootCtx          context.Context
	newListService   func(app.ListServiceConfig) listTaskRunner
	newDetailService func(app.DetailServiceConfig) detailTaskRunner
	newReviewService func(app.ReviewServiceConfig) reviewTaskRunner
	dbLockSet        *serveDBLockSet

	mu    sync.Mutex
	order []string
	tasks map[string]*TaskView
}

func NewTaskManager(rootCtx context.Context, cfg Config) *TaskManager {
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	return &TaskManager{
		cfg:       cfg,
		rootCtx:   rootCtx,
		dbLockSet: newServeDBLockSet(),
		newListService: func(cfg app.ListServiceConfig) listTaskRunner {
			return app.NewListService(cfg)
		},
		newDetailService: func(cfg app.DetailServiceConfig) detailTaskRunner {
			return app.NewDetailService(cfg)
		},
		newReviewService: func(cfg app.ReviewServiceConfig) reviewTaskRunner {
			return app.NewReviewService(cfg)
		},
		tasks: make(map[string]*TaskView),
	}
}

func (m *TaskManager) List() []TaskView {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]TaskView, 0, len(m.order))
	for i := len(m.order) - 1; i >= 0; i-- {
		if task, ok := m.tasks[m.order[i]]; ok {
			result = append(result, *task)
		}
	}
	return result
}

func (m *TaskManager) Get(id string) (TaskView, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[id]
	if !ok {
		return TaskView{}, false
	}
	return *task, true
}

func (m *TaskManager) SubmitList(cfg config.ListCommandConfig) (TaskView, error) {
	task := m.createTask("list")
	go m.runListTask(task.ID, cfg)
	return task, nil
}

func (m *TaskManager) SubmitDetail(cfg config.DetailCommandConfig) (TaskView, error) {
	task := m.createTask("detail")
	go m.runDetailTask(task.ID, cfg)
	return task, nil
}

func (m *TaskManager) SubmitReview(cfg config.ReviewCommandConfig) (TaskView, error) {
	task := m.createTask("reviews")
	go m.runReviewTask(task.ID, cfg)
	return task, nil
}

func (m *TaskManager) createTask(kind string) TaskView {
	task := TaskView{
		ID:        uuid.NewString(),
		Kind:      kind,
		Status:    TaskStatusPending,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	m.mu.Lock()
	m.tasks[task.ID] = &task
	m.order = append(m.order, task.ID)
	m.mu.Unlock()
	return task
}

func (m *TaskManager) runListTask(id string, cfg config.ListCommandConfig) {
	unlock := m.dbLockSet.Lock(cfg.DBPath)
	defer unlock()

	m.markRunning(id)
	ctx, cancel := context.WithTimeout(m.rootCtx, 10*time.Minute)
	defer cancel()

	service := m.newListService(app.ListServiceConfig{
		BaseURL:        firstNonEmptyString(m.cfg.BaseURL, config.DefaultBaseURL),
		BackendBaseURL: firstNonEmptyString(m.cfg.BackendBaseURL, config.DefaultBackendBaseURL),
		Source:         cfg.Source,
		DBPath:         cfg.DBPath,
		Debug:          cfg.Debug,
		MaxRetries:     cfg.MaxRetries,
		ProxyURLs:      cfg.ProxyURLs,
		RunSource:      "serve",
		TaskName:       fmt.Sprintf("%s-%s", cfg.Task.Category, cfg.Task.Metric),
	})

	result, err := service.Run(ctx, cfg.Task)
	outcome := map[string]any{
		"pages_visited":           result.PagesVisited,
		"pages_scheduled":         result.PagesScheduled,
		"pages_succeeded":         result.PagesSucceeded,
		"pages_written":           result.PagesWritten,
		"works_upserted":          result.WorksUpserted,
		"list_entries_inserted":   result.ListEntriesInserted,
		"latest_entries_upserted": result.LatestEntriesUpserted,
		"failures":                result.Failures,
	}
	m.finishTask(id, result.RunID, result.RequestedSource, result.EffectiveSource, result.FallbackUsed, result.FallbackReason, outcome, err)
}

func (m *TaskManager) runDetailTask(id string, cfg config.DetailCommandConfig) {
	unlock := m.dbLockSet.Lock(cfg.DBPath)
	defer unlock()

	m.markRunning(id)
	ctx, cancel := context.WithTimeout(m.rootCtx, 30*time.Minute)
	defer cancel()

	service := m.newDetailService(app.DetailServiceConfig{
		BaseURL:        firstNonEmptyString(m.cfg.BaseURL, config.DefaultBaseURL),
		BackendBaseURL: firstNonEmptyString(m.cfg.BackendBaseURL, config.DefaultBackendBaseURL),
		Source:         cfg.Source,
		DBPath:         cfg.DBPath,
		Debug:          cfg.Debug,
		MaxRetries:     cfg.MaxRetries,
		ProxyURLs:      cfg.ProxyURLs,
	})

	result, err := service.Run(ctx, cfg.Task)
	outcome := map[string]any{
		"total":             result.Total,
		"processed":         result.Processed,
		"fetched":           result.Fetched,
		"skipped":           result.Skipped,
		"failed":            result.Failed,
		"recovered_running": result.RecoveredRunning,
		"details_upserted":  result.DetailsUpserted,
		"enrich_ok":         result.EnrichSucceeded,
		"enrich_failed":     result.EnrichFailed,
		"enrich_skipped":    result.EnrichSkipped,
		"failures":          result.Failures,
	}
	m.finishTask(id, result.RunID, result.RequestedSource, result.EffectiveSource, result.FallbackUsed, result.FallbackReason, outcome, err)
}

func (m *TaskManager) runReviewTask(id string, cfg config.ReviewCommandConfig) {
	unlock := m.dbLockSet.Lock(cfg.DBPath)
	defer unlock()

	m.markRunning(id)
	ctx, cancel := context.WithTimeout(m.rootCtx, 30*time.Minute)
	defer cancel()

	service := m.newReviewService(app.ReviewServiceConfig{
		BaseURL:    firstNonEmptyString(m.cfg.BackendBaseURL, config.DefaultBackendBaseURL),
		DBPath:     cfg.DBPath,
		Debug:      cfg.Debug,
		MaxRetries: cfg.MaxRetries,
		ProxyURLs:  cfg.ProxyURLs,
	})

	result, err := service.Run(ctx, cfg.Task)
	outcome := map[string]any{
		"candidates":              result.Candidates,
		"scopes_scheduled":        result.ScopesScheduled,
		"scopes_processed":        result.ScopesProcessed,
		"scopes_fetched":          result.ScopesFetched,
		"scopes_skipped":          result.ScopesSkipped,
		"scopes_failed":           result.ScopesFailed,
		"reviews_fetched":         result.ReviewsFetched,
		"review_snapshots_saved":  result.ReviewSnapshotsSaved,
		"latest_reviews_upserted": result.LatestReviewsUpserted,
		"failures":                result.Failures,
	}
	m.finishTask(id, result.RunID, result.RequestedSource, result.EffectiveSource, result.FallbackUsed, result.FallbackReason, outcome, err)
}

func (m *TaskManager) markRunning(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[id]; ok {
		task.Status = TaskStatusRunning
		task.StartedAt = time.Now().UTC().Format(time.RFC3339)
	}
}

func (m *TaskManager) finishTask(id string, runID string, requestedSource string, effectiveSource string, fallbackUsed bool, fallbackReason string, outcome map[string]any, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[id]
	if !ok {
		return
	}
	task.RunID = runID
	task.RequestedSource = requestedSource
	task.EffectiveSource = effectiveSource
	task.FallbackUsed = fallbackUsed
	task.FallbackReason = fallbackReason
	task.Outcome = outcome
	task.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	if err != nil {
		task.Status = TaskStatusFailed
		task.Error = err.Error()
		return
	}
	task.Status = TaskStatusSucceeded
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

type listTaskRunner interface {
	Run(context.Context, domain.ListTask) (app.ListRunResult, error)
}

type detailTaskRunner interface {
	Run(context.Context, domain.DetailTask) (app.DetailRunResult, error)
}

type reviewTaskRunner interface {
	Run(context.Context, domain.ReviewTask) (app.ReviewRunResult, error)
}

type serveDBLockSet struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newServeDBLockSet() *serveDBLockSet {
	return &serveDBLockSet{locks: make(map[string]*sync.Mutex)}
}

func (s *serveDBLockSet) Lock(dbPath string) func() {
	key := filepath.Clean(dbPath)
	s.mu.Lock()
	lock, ok := s.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		s.locks[key] = lock
	}
	s.mu.Unlock()

	lock.Lock()
	return func() { lock.Unlock() }
}
