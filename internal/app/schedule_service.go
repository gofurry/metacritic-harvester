package app

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/gofurry/metacritic-harvester/internal/config"
)

type scheduleBatchRunner interface {
	RunWithConcurrency(context.Context, []config.BatchTaskConfig, int) BatchRunResult
}

type ScheduleService struct {
	baseURL             string
	loadBatchFile       func(string) (config.BatchFile, error)
	buildBatchRunConfig func(config.BatchFile, int) (config.BatchRunConfig, error)
	newBatchService     func(string) scheduleBatchRunner
	logf                func(string, ...any)
}

func NewScheduleService(baseURL string) *ScheduleService {
	return &ScheduleService{
		baseURL:             baseURL,
		loadBatchFile:       config.LoadBatchFile,
		buildBatchRunConfig: config.BuildBatchRunConfig,
		newBatchService: func(baseURL string) scheduleBatchRunner {
			return NewBatchService(baseURL)
		},
		logf: log.Printf,
	}
}

func (s *ScheduleService) Run(ctx context.Context, schedule config.ScheduleFile) error {
	location := time.Local
	if schedule.Timezone != "" {
		loaded, err := time.LoadLocation(schedule.Timezone)
		if err != nil {
			return fmt.Errorf("load timezone: %w", err)
		}
		location = loaded
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
	scheduler := cron.New(
		cron.WithLocation(location),
		cron.WithParser(parser),
	)

	var runWG sync.WaitGroup
	registeredJobs := 0
	batchRunner := s.newBatchService(s.baseURL)

	for _, job := range schedule.Jobs {
		if !job.IsEnabled() {
			s.logf("schedule skip disabled job=%s", job.Name)
			continue
		}

		job := job
		var running bool
		var runningMu sync.Mutex
		entryID, err := scheduler.AddFunc(job.Cron, func() {
			runningMu.Lock()
			if running {
				runningMu.Unlock()
				s.logf("schedule skip overlapping job=%s", job.Name)
				return
			}
			running = true
			runningMu.Unlock()

			runWG.Add(1)
			defer runWG.Done()
			defer func() {
				runningMu.Lock()
				running = false
				runningMu.Unlock()
			}()

			startedAt := time.Now().In(location)
			s.logf("schedule start job=%s batch_file=%s started_at=%s", job.Name, job.BatchFile, startedAt.Format(time.RFC3339))

			batchFile, err := s.loadBatchFile(job.BatchFile)
			if err != nil {
				s.logf("schedule failed job=%s err=%v", job.Name, err)
				return
			}

			concurrency := 0
			if job.Concurrency != nil {
				concurrency = *job.Concurrency
			}

			runConfig, err := s.buildBatchRunConfig(batchFile, concurrency)
			if err != nil {
				s.logf("schedule failed job=%s err=%v", job.Name, err)
				return
			}

			result := batchRunner.RunWithConcurrency(ctx, runConfig.Tasks, runConfig.Concurrency)
			s.logf(
				"schedule finish job=%s total=%d succeeded=%d failed=%d pages_scheduled=%d pages_succeeded=%d pages_written=%d works=%d list_entries=%d latest_entries=%d detail_processed=%d detail_fetched=%d detail_skipped=%d recovered_running=%d details_upserted=%d failures=%d",
				job.Name,
				result.TotalTasks,
				result.SucceededTasks,
				result.FailedTasks,
				result.TotalPagesScheduled,
				result.TotalPagesSucceeded,
				result.TotalPagesWritten,
				result.TotalWorksUpserted,
				result.TotalListEntriesInserted,
				result.TotalLatestEntriesUpserted,
				result.TotalDetailProcessed,
				result.TotalDetailFetched,
				result.TotalDetailSkipped,
				result.TotalRecoveredRunning,
				result.TotalDetailsUpserted,
				result.TotalFailures,
			)
		})
		if err != nil {
			return fmt.Errorf("register schedule job %s: %w", job.Name, err)
		}

		entry := scheduler.Entry(entryID)
		s.logf("schedule registered job=%s cron=%s next=%s", job.Name, job.Cron, entry.Next.In(location).Format(time.RFC3339))
		registeredJobs++
	}

	if registeredJobs == 0 {
		return fmt.Errorf("schedule file does not contain any enabled jobs")
	}

	scheduler.Start()
	defer func() {
		stopCtx := scheduler.Stop()
		<-stopCtx.Done()
		runWG.Wait()
	}()

	<-ctx.Done()
	return nil
}
