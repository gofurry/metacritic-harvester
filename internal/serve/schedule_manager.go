package serve

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"github.com/GoFurry/metacritic-harvester/internal/config"
)

type scheduleActiveView struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	File           string `json:"file"`
	Active         bool   `json:"active"`
	StartedAt      string `json:"started_at,omitempty"`
	StoppedAt      string `json:"stopped_at,omitempty"`
	LastTickAt     string `json:"last_tick_at,omitempty"`
	LastError      string `json:"last_error,omitempty"`
	JobsRegistered int    `json:"jobs_registered"`
	TaskID         string `json:"task_id,omitempty"`
}

type ScheduleManager struct {
	cfg    Config
	root   context.Context
	tasks  *TaskManager
	logger *log.Logger

	mu      sync.Mutex
	active  map[string]*activeSchedule
	history map[string]scheduleActiveView
}

type activeSchedule struct {
	view      scheduleActiveView
	cron      *cron.Cron
	cancel    context.CancelFunc
	done      chan struct{}
	taskID    string
	entryJobs []string
}

func NewScheduleManager(root context.Context, cfg Config, tasks *TaskManager) *ScheduleManager {
	if root == nil {
		root = context.Background()
	}
	return &ScheduleManager{
		cfg:     cfg,
		root:    root,
		tasks:   tasks,
		logger:  log.Default(),
		active:  make(map[string]*activeSchedule),
		history: make(map[string]scheduleActiveView),
	}
}

func (m *ScheduleManager) ListActive() []scheduleActiveView {
	m.mu.Lock()
	defer m.mu.Unlock()
	rows := make([]scheduleActiveView, 0, len(m.active))
	for _, runtime := range m.active {
		rows = append(rows, runtime.view)
	}
	sortScheduleViews(rows)
	return rows
}

func (m *ScheduleManager) Start(name string, relPath string, schedule config.ScheduleFile) (scheduleActiveView, error) {
	m.mu.Lock()
	if _, exists := m.active[relPath]; exists {
		m.mu.Unlock()
		return scheduleActiveView{}, fmt.Errorf("schedule is already active")
	}
	task := m.tasks.createTask("schedule")
	m.tasks.markRunning(task.ID)
	ctx, cancel := context.WithCancel(m.root)
	view := scheduleActiveView{
		ID:             uuid.NewString(),
		Name:           name,
		File:           relPath,
		Active:         true,
		StartedAt:      time.Now().UTC().Format(time.RFC3339),
		JobsRegistered: countEnabledJobs(schedule),
		TaskID:         task.ID,
	}
	runtime := &activeSchedule{
		view:   view,
		cancel: cancel,
		done:   make(chan struct{}),
		taskID: task.ID,
	}
	m.active[relPath] = runtime
	m.history[relPath] = view
	m.mu.Unlock()

	go m.run(ctx, relPath, schedule, runtime)
	return view, nil
}

func (m *ScheduleManager) Stop(relPath string) (scheduleActiveView, error) {
	m.mu.Lock()
	runtime, ok := m.active[relPath]
	if !ok {
		m.mu.Unlock()
		if history, exists := m.history[relPath]; exists {
			return history, fmt.Errorf("schedule is not active")
		}
		return scheduleActiveView{}, fmt.Errorf("schedule is not active")
	}
	cancel := runtime.cancel
	done := runtime.done
	m.mu.Unlock()

	cancel()
	<-done

	m.mu.Lock()
	defer m.mu.Unlock()
	view, ok := m.history[relPath]
	if !ok {
		return scheduleActiveView{}, fmt.Errorf("schedule is not active")
	}
	return view, nil
}

func (m *ScheduleManager) run(ctx context.Context, relPath string, schedule config.ScheduleFile, runtime *activeSchedule) {
	defer close(runtime.done)

	location := time.Local
	if strings.TrimSpace(schedule.Timezone) != "" {
		if loaded, err := time.LoadLocation(schedule.Timezone); err == nil {
			location = loaded
		}
	}
	parser := cron.NewParser(
		cron.SecondOptional |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor,
	)
	scheduler := cron.New(cron.WithLocation(location), cron.WithParser(parser))
	for _, job := range schedule.Jobs {
		if !job.IsEnabled() {
			continue
		}
		job := job
		_, err := scheduler.AddFunc(job.Cron, func() {
			now := time.Now().UTC().Format(time.RFC3339)
			m.updateLastTick(relPath, now)
			m.logger.Printf("serve schedule tick: file=%s job=%s batch_file=%s", relPath, job.Name, job.BatchFile)

			batchFile, err := config.LoadBatchFile(job.BatchFile)
			if err != nil {
				m.setScheduleError(relPath, fmt.Sprintf("load batch file: %v", err))
				return
			}
			runCfg, err := config.BuildBatchRunConfig(batchFile, derefInt(job.Concurrency))
			if err != nil {
				m.setScheduleError(relPath, fmt.Sprintf("build batch run config: %v", err))
				return
			}
			if _, err := m.tasks.SubmitBatch(filepathBaseForward(job.BatchFile), runCfg); err != nil {
				m.setScheduleError(relPath, fmt.Sprintf("submit batch: %v", err))
				return
			}
		})
		if err != nil {
			m.setScheduleError(relPath, fmt.Sprintf("register job %s: %v", job.Name, err))
			break
		}
	}
	scheduler.Start()
	defer func() {
		stopCtx := scheduler.Stop()
		<-stopCtx.Done()
	}()

	<-ctx.Done()
	m.finishSchedule(relPath, "")
}

func (m *ScheduleManager) updateLastTick(relPath string, tick string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime, ok := m.active[relPath]; ok {
		runtime.view.LastTickAt = tick
		m.history[relPath] = runtime.view
	}
}

func (m *ScheduleManager) setScheduleError(relPath string, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if runtime, ok := m.active[relPath]; ok {
		runtime.view.LastError = message
		m.history[relPath] = runtime.view
	}
}

func (m *ScheduleManager) finishSchedule(relPath string, errText string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, ok := m.active[relPath]
	if !ok {
		return
	}
	runtime.view.Active = false
	runtime.view.StoppedAt = time.Now().UTC().Format(time.RFC3339)
	if errText != "" {
		runtime.view.LastError = errText
	}
	m.history[relPath] = runtime.view
	delete(m.active, relPath)
	m.tasks.finishTask(runtime.taskID, "", "api", "api", false, "", map[string]any{
		"schedule_name":   runtime.view.Name,
		"file":            runtime.view.File,
		"active":          runtime.view.Active,
		"started_at":      runtime.view.StartedAt,
		"stopped_at":      runtime.view.StoppedAt,
		"last_tick_at":    runtime.view.LastTickAt,
		"last_error":      runtime.view.LastError,
		"jobs_registered": runtime.view.JobsRegistered,
	}, nil)
}

func countEnabledJobs(schedule config.ScheduleFile) int {
	count := 0
	for _, job := range schedule.Jobs {
		if job.IsEnabled() {
			count++
		}
	}
	return count
}

func derefInt(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func filepathBaseForward(path string) string {
	path = strings.ReplaceAll(path, "\\", "/")
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func sortScheduleViews(rows []scheduleActiveView) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Active != rows[j].Active {
			return rows[i].Active
		}
		return rows[i].File < rows[j].File
	})
}
