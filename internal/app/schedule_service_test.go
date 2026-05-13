package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestScheduleServiceRunTriggersBatch(t *testing.T) {
	t.Parallel()

	triggered := make(chan struct{}, 1)
	service := NewScheduleService("https://example.com")
	service.logf = func(string, ...any) {}
	service.loadBatchFile = func(string) (config.BatchFile, error) {
		return config.BatchFile{
			Tasks: []config.BatchTaskSpec{{Category: "game", Metric: "metascore"}},
		}, nil
	}
	service.buildBatchRunConfig = func(file config.BatchFile, override int) (config.BatchRunConfig, error) {
		return config.BatchRunConfig{
			Tasks: []config.BatchTaskConfig{
				{
					Name: "scheduled-task",
					Kind: config.BatchTaskKindList,
					List: &config.ListCommandConfig{
						Task: domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore},
					},
				},
			},
			Concurrency: 1,
		}, nil
	}
	service.newBatchService = func(string) scheduleBatchRunner {
		return scheduleBatchRunnerFunc(func(context.Context, []config.BatchTaskConfig, int) BatchRunResult {
			select {
			case triggered <- struct{}{}:
			default:
			}
			return BatchRunResult{TotalTasks: 1, SucceededTasks: 1}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- service.Run(ctx, config.ScheduleFile{
			Jobs: []config.ScheduleJob{
				{
					Name:      "tick",
					Cron:      "*/1 * * * * *",
					BatchFile: "tasks.yaml",
				},
			},
		})
	}()

	select {
	case <-triggered:
		cancel()
	case <-time.After(3 * time.Second):
		t.Fatal("expected scheduled batch to trigger")
	}

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected schedule service to stop after cancel")
	}
}

func TestScheduleServiceSkipsOverlappingJob(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{}, 1)
	skipped := make(chan struct{}, 1)
	var runs int32

	service := NewScheduleService("https://example.com")
	service.logf = func(format string, args ...any) {
		if format == "schedule skip overlapping job=%s" {
			select {
			case skipped <- struct{}{}:
			default:
			}
		}
	}
	service.loadBatchFile = func(string) (config.BatchFile, error) {
		return config.BatchFile{
			Tasks: []config.BatchTaskSpec{{Category: "game", Metric: "metascore"}},
		}, nil
	}
	service.buildBatchRunConfig = func(file config.BatchFile, override int) (config.BatchRunConfig, error) {
		return config.BatchRunConfig{
			Tasks: []config.BatchTaskConfig{
				{
					Name: "scheduled-task",
					Kind: config.BatchTaskKindList,
					List: &config.ListCommandConfig{
						Task: domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore},
					},
				},
			},
			Concurrency: 1,
		}, nil
	}
	service.newBatchService = func(string) scheduleBatchRunner {
		return scheduleBatchRunnerFunc(func(ctx context.Context, _ []config.BatchTaskConfig, _ int) BatchRunResult {
			atomic.AddInt32(&runs, 1)
			select {
			case firstStarted <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return BatchRunResult{TotalTasks: 1, FailedTasks: 1}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- service.Run(ctx, config.ScheduleFile{
			Jobs: []config.ScheduleJob{
				{
					Name:      "tick",
					Cron:      "*/1 * * * * *",
					BatchFile: "tasks.yaml",
				},
			},
		})
	}()

	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("expected first scheduled batch to trigger")
	}

	select {
	case <-skipped:
	case <-time.After(3 * time.Second):
		t.Fatal("expected overlapping trigger to be skipped")
	}

	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("expected one running batch, got %d", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected schedule service to stop after cancel")
	}
}

func TestScheduleServicePassesParentContextToBatch(t *testing.T) {
	t.Parallel()

	batchCtx := make(chan context.Context, 1)
	service := NewScheduleService("https://example.com")
	service.logf = func(string, ...any) {}
	service.loadBatchFile = func(string) (config.BatchFile, error) {
		return config.BatchFile{
			Tasks: []config.BatchTaskSpec{{Category: "game", Metric: "metascore"}},
		}, nil
	}
	service.buildBatchRunConfig = func(file config.BatchFile, override int) (config.BatchRunConfig, error) {
		return config.BatchRunConfig{
			Tasks: []config.BatchTaskConfig{
				{
					Name: "scheduled-task",
					Kind: config.BatchTaskKindList,
					List: &config.ListCommandConfig{
						Task: domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore},
					},
				},
			},
			Concurrency: 1,
		}, nil
	}
	service.newBatchService = func(string) scheduleBatchRunner {
		return scheduleBatchRunnerFunc(func(ctx context.Context, _ []config.BatchTaskConfig, _ int) BatchRunResult {
			batchCtx <- ctx
			<-ctx.Done()
			return BatchRunResult{TotalTasks: 1, FailedTasks: 1}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- service.Run(ctx, config.ScheduleFile{
			Jobs: []config.ScheduleJob{
				{
					Name:      "tick",
					Cron:      "*/1 * * * * *",
					BatchFile: "tasks.yaml",
				},
			},
		})
	}()

	var runCtx context.Context
	select {
	case runCtx = <-batchCtx:
	case <-time.After(3 * time.Second):
		t.Fatal("expected scheduled batch to receive context")
	}

	cancel()
	select {
	case <-runCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("expected batch context to be canceled by parent context")
	}

	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected schedule service to stop after cancel")
	}
}

type scheduleBatchRunnerFunc func(context.Context, []config.BatchTaskConfig, int) BatchRunResult

func (f scheduleBatchRunnerFunc) RunWithConcurrency(ctx context.Context, tasks []config.BatchTaskConfig, concurrency int) BatchRunResult {
	return f(ctx, tasks, concurrency)
}
