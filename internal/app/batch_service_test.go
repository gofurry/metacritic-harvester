package app

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestBatchRunResultError(t *testing.T) {
	t.Parallel()

	result := BatchRunResult{}
	if err := result.Error(); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	result.FailedTasks = 1
	if err := result.Error(); err == nil {
		t.Fatal("expected non-nil error")
	}
}

func TestBatchServiceRunAggregatesMixedTasks(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	service := NewBatchService("https://example.com")
	service.newListService = func(cfg ListServiceConfig) listTaskRunner {
		return listTaskRunnerFunc(func(_ context.Context, task domain.ListTask) (ListRunResult, error) {
			return ListRunResult{
				RunID:                 "list-run",
				PagesVisited:          task.MaxPages,
				PagesScheduled:        task.MaxPages,
				PagesSucceeded:        task.MaxPages,
				PagesWritten:          task.MaxPages,
				WorksUpserted:         2,
				ListEntriesInserted:   4,
				LatestEntriesUpserted: 4,
			}, nil
		})
	}
	service.newDetailService = func(cfg DetailServiceConfig) detailTaskRunner {
		return detailTaskRunnerFunc(func(_ context.Context, task domain.DetailTask) (DetailRunResult, error) {
			return DetailRunResult{
				RunID:            "detail-run",
				Total:            3,
				Processed:        3,
				Fetched:          2,
				Skipped:          1,
				RecoveredRunning: 1,
				DetailsUpserted:  2,
			}, nil
		})
	}

	result := service.Run(context.Background(), []config.BatchTaskConfig{
		{
			Name: "list-task",
			Kind: config.BatchTaskKindList,
			List: &config.ListCommandConfig{
				Task:   domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore, MaxPages: 2},
				DBPath: base + "/one.db",
			},
		},
		{
			Name: "detail-task",
			Kind: config.BatchTaskKindDetail,
			Detail: &config.DetailCommandConfig{
				Task:        domain.DetailTask{Category: domain.CategoryMovie, Concurrency: 2},
				DBPath:      base + "/two.db",
				Concurrency: 2,
			},
		},
	})

	if result.TotalTasks != 2 || result.SucceededTasks != 2 || result.FailedTasks != 0 {
		t.Fatalf("unexpected summary: %+v", result)
	}
	if result.TotalPagesScheduled != 2 || result.TotalPagesSucceeded != 2 || result.TotalPagesWritten != 2 {
		t.Fatalf("unexpected list page aggregates: %+v", result)
	}
	if result.TotalWorksUpserted != 2 || result.TotalListEntriesInserted != 4 || result.TotalLatestEntriesUpserted != 4 {
		t.Fatalf("unexpected list aggregates: %+v", result)
	}
	if result.TotalDetailProcessed != 3 || result.TotalDetailFetched != 2 || result.TotalDetailSkipped != 1 || result.TotalRecoveredRunning != 1 || result.TotalDetailsUpserted != 2 {
		t.Fatalf("unexpected detail aggregates: %+v", result)
	}
	if len(result.Tasks) != 2 || result.Tasks[0].Kind != "list" || result.Tasks[1].Kind != "detail" {
		t.Fatalf("unexpected task results: %+v", result.Tasks)
	}
}

func TestBatchServiceRunPreservesFailedListTaskCounts(t *testing.T) {
	t.Parallel()

	service := NewBatchService("https://example.com")
	service.newListService = func(cfg ListServiceConfig) listTaskRunner {
		return listTaskRunnerFunc(func(_ context.Context, task domain.ListTask) (ListRunResult, error) {
			return ListRunResult{
				RunID:                 "failed-run",
				PagesVisited:          1,
				PagesScheduled:        2,
				PagesSucceeded:        1,
				PagesWritten:          1,
				WorksUpserted:         1,
				ListEntriesInserted:   1,
				LatestEntriesUpserted: 0,
				Failures:              1,
			}, assertErr("partial failure")
		})
	}

	result := service.Run(context.Background(), []config.BatchTaskConfig{
		{
			Name: "task-1",
			Kind: config.BatchTaskKindList,
			List: &config.ListCommandConfig{
				Task:   domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore},
				DBPath: "failed.db",
			},
		},
	})

	if result.SucceededTasks != 0 || result.FailedTasks != 1 {
		t.Fatalf("unexpected summary: %+v", result)
	}
	if result.TotalPagesScheduled != 2 || result.TotalPagesSucceeded != 1 || result.TotalPagesWritten != 1 {
		t.Fatalf("unexpected page aggregate counts: %+v", result)
	}
	if result.TotalWorksUpserted != 1 || result.TotalListEntriesInserted != 1 || result.TotalLatestEntriesUpserted != 0 || result.TotalFailures != 1 {
		t.Fatalf("unexpected aggregate counts: %+v", result)
	}
	if len(result.Tasks) != 1 || result.Tasks[0].Success {
		t.Fatalf("unexpected task result: %+v", result.Tasks)
	}
	if result.Tasks[0].RunID != "failed-run" || result.Tasks[0].Failures != 1 {
		t.Fatalf("expected failed task details to be preserved, got %+v", result.Tasks[0])
	}
}

func TestBatchServiceRunPreservesFailedDetailTaskCounts(t *testing.T) {
	t.Parallel()

	service := NewBatchService("https://example.com")
	service.newDetailService = func(cfg DetailServiceConfig) detailTaskRunner {
		return detailTaskRunnerFunc(func(_ context.Context, task domain.DetailTask) (DetailRunResult, error) {
			return DetailRunResult{
				RunID:            "detail-failed-run",
				Total:            4,
				Processed:        4,
				Fetched:          2,
				Skipped:          1,
				Failed:           1,
				RecoveredRunning: 1,
				DetailsUpserted:  2,
				Failures:         1,
			}, assertErr("detail partial failure")
		})
	}

	result := service.Run(context.Background(), []config.BatchTaskConfig{
		{
			Name: "detail-task",
			Kind: config.BatchTaskKindDetail,
			Detail: &config.DetailCommandConfig{
				Task:        domain.DetailTask{Category: domain.CategoryTV, Concurrency: 3},
				DBPath:      "detail.db",
				Concurrency: 3,
			},
		},
	})

	if result.SucceededTasks != 0 || result.FailedTasks != 1 {
		t.Fatalf("unexpected summary: %+v", result)
	}
	if result.TotalDetailProcessed != 4 || result.TotalDetailFetched != 2 || result.TotalDetailSkipped != 1 || result.TotalRecoveredRunning != 1 || result.TotalDetailsUpserted != 2 || result.TotalFailures != 1 {
		t.Fatalf("unexpected detail aggregate counts: %+v", result)
	}
	if len(result.Tasks) != 1 || result.Tasks[0].Success {
		t.Fatalf("unexpected task result: %+v", result.Tasks)
	}
	if result.Tasks[0].RunID != "detail-failed-run" || result.Tasks[0].Failures != 1 {
		t.Fatalf("expected failed detail task details to be preserved, got %+v", result.Tasks[0])
	}
}

func TestBatchServiceRunWithConcurrency(t *testing.T) {
	t.Parallel()

	service := NewBatchService("https://example.com")
	var active int32
	var maxActive int32
	service.newListService = func(cfg ListServiceConfig) listTaskRunner {
		return listTaskRunnerFunc(func(_ context.Context, task domain.ListTask) (ListRunResult, error) {
			current := atomic.AddInt32(&active, 1)
			for {
				prev := atomic.LoadInt32(&maxActive)
				if current <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&active, -1)
			return ListRunResult{RunID: string(task.Metric), PagesScheduled: 1, PagesSucceeded: 1, PagesWritten: 1, WorksUpserted: 1}, nil
		})
	}

	result := service.RunWithConcurrency(context.Background(), []config.BatchTaskConfig{
		{Name: "one", Kind: config.BatchTaskKindList, List: &config.ListCommandConfig{Task: domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore}, DBPath: "one.db"}},
		{Name: "two", Kind: config.BatchTaskKindList, List: &config.ListCommandConfig{Task: domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricUserScore}, DBPath: "two.db"}},
	}, 2)

	if result.SucceededTasks != 2 || result.FailedTasks != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if maxActive < 2 {
		t.Fatalf("expected concurrent execution, max active=%d", maxActive)
	}
}

func TestBatchServiceRunWithConcurrencySerializesSameDB(t *testing.T) {
	t.Parallel()

	service := NewBatchService("https://example.com")
	var active int32
	var maxActive int32
	service.newDetailService = func(cfg DetailServiceConfig) detailTaskRunner {
		return detailTaskRunnerFunc(func(_ context.Context, task domain.DetailTask) (DetailRunResult, error) {
			current := atomic.AddInt32(&active, 1)
			for {
				prev := atomic.LoadInt32(&maxActive)
				if current <= prev || atomic.CompareAndSwapInt32(&maxActive, prev, current) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&active, -1)
			return DetailRunResult{RunID: "detail-run", Processed: 1, Fetched: 1, DetailsUpserted: 1}, nil
		})
	}

	result := service.RunWithConcurrency(context.Background(), []config.BatchTaskConfig{
		{Name: "one", Kind: config.BatchTaskKindDetail, Detail: &config.DetailCommandConfig{Task: domain.DetailTask{Category: domain.CategoryGame, Concurrency: 1}, DBPath: "shared.db", Concurrency: 1}},
		{Name: "two", Kind: config.BatchTaskKindDetail, Detail: &config.DetailCommandConfig{Task: domain.DetailTask{Category: domain.CategoryGame, Concurrency: 1}, DBPath: "shared.db", Concurrency: 1}},
	}, 2)

	if result.SucceededTasks != 2 || result.FailedTasks != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if maxActive != 1 {
		t.Fatalf("expected same-db tasks to run serially, max active=%d", maxActive)
	}
}

type assertErr string

func (e assertErr) Error() string { return string(e) }
