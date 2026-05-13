package serve

import (
	"strings"

	"github.com/gofurry/metacritic-harvester/internal/config"
)

type batchTaskSpecView struct {
	Name        string   `json:"name"`
	Kind        string   `json:"kind"`
	Source      string   `json:"source,omitempty"`
	Category    string   `json:"category,omitempty"`
	Metric      string   `json:"metric,omitempty"`
	Year        string   `json:"year,omitempty"`
	Platform    []string `json:"platform,omitempty"`
	Network     []string `json:"network,omitempty"`
	Genre       []string `json:"genre,omitempty"`
	ReleaseType []string `json:"release_type,omitempty"`
	Pages       int      `json:"pages,omitempty"`
	WorkHref    string   `json:"work_href,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	Force       bool     `json:"force,omitempty"`
	ReviewType  string   `json:"review_type,omitempty"`
	Sentiment   string   `json:"sentiment,omitempty"`
	Sort        string   `json:"sort,omitempty"`
	PageSize    int      `json:"page_size,omitempty"`
	MaxPages    int      `json:"max_pages,omitempty"`
	DBPath      string   `json:"db_path,omitempty"`
	Concurrency int      `json:"concurrency,omitempty"`
}

type batchFileDetailView struct {
	File        managedFileView     `json:"file"`
	DefaultDB   string              `json:"default_db,omitempty"`
	Concurrency int                 `json:"concurrency,omitempty"`
	TaskCount   int                 `json:"task_count"`
	Tasks       []batchTaskSpecView `json:"tasks"`
}

type scheduleJobView struct {
	Name        string `json:"name"`
	Cron        string `json:"cron"`
	BatchFile   string `json:"batch_file"`
	Enabled     bool   `json:"enabled"`
	Concurrency int    `json:"concurrency,omitempty"`
}

type scheduleFileDetailView struct {
	File     managedFileView     `json:"file"`
	Timezone string              `json:"timezone,omitempty"`
	JobCount int                 `json:"job_count"`
	Jobs     []scheduleJobView   `json:"jobs"`
	Active   *scheduleActiveView `json:"active,omitempty"`
}

func mapBatchFileDetail(file managedFileView, batchFile config.BatchFile) batchFileDetailView {
	tasks := make([]batchTaskSpecView, 0, len(batchFile.Tasks))
	for _, task := range batchFile.Tasks {
		view := batchTaskSpecView{
			Name:        strings.TrimSpace(task.Name),
			Kind:        strings.TrimSpace(task.Kind),
			Source:      strings.TrimSpace(task.Source),
			Category:    strings.TrimSpace(task.Category),
			Metric:      strings.TrimSpace(task.Metric),
			Year:        strings.TrimSpace(task.Year),
			Platform:    task.Platform,
			Network:     task.Network,
			Genre:       task.Genre,
			ReleaseType: task.ReleaseType,
			WorkHref:    strings.TrimSpace(task.WorkHref),
			ReviewType:  strings.TrimSpace(task.ReviewType),
			Sentiment:   strings.TrimSpace(task.Sentiment),
			Sort:        strings.TrimSpace(task.Sort),
			DBPath:      strings.TrimSpace(task.DBPath),
		}
		if task.Pages != nil {
			view.Pages = *task.Pages
		}
		if task.Limit != nil {
			view.Limit = *task.Limit
		}
		if task.Force != nil {
			view.Force = *task.Force
		}
		if task.PageSize != nil {
			view.PageSize = *task.PageSize
		}
		if task.MaxPages != nil {
			view.MaxPages = *task.MaxPages
		}
		if task.DetailConcurrency != nil {
			view.Concurrency = *task.DetailConcurrency
		} else if task.ReviewConcurrency != nil {
			view.Concurrency = *task.ReviewConcurrency
		}
		tasks = append(tasks, view)
	}
	concurrency := 0
	if batchFile.Defaults.Concurrency != nil {
		concurrency = *batchFile.Defaults.Concurrency
	}
	return batchFileDetailView{
		File:        file,
		DefaultDB:   strings.TrimSpace(batchFile.Defaults.DBPath),
		Concurrency: concurrency,
		TaskCount:   len(batchFile.Tasks),
		Tasks:       tasks,
	}
}

func mapScheduleFileDetail(file managedFileView, schedule config.ScheduleFile, active *scheduleActiveView) scheduleFileDetailView {
	jobs := make([]scheduleJobView, 0, len(schedule.Jobs))
	for _, job := range schedule.Jobs {
		view := scheduleJobView{
			Name:      job.Name,
			Cron:      job.Cron,
			BatchFile: job.BatchFile,
			Enabled:   job.IsEnabled(),
		}
		if job.Concurrency != nil {
			view.Concurrency = *job.Concurrency
		}
		jobs = append(jobs, view)
	}
	return scheduleFileDetailView{
		File:     file,
		Timezone: strings.TrimSpace(schedule.Timezone),
		JobCount: len(schedule.Jobs),
		Jobs:     jobs,
		Active:   active,
	}
}
