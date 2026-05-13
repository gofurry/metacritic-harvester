package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/gofurry/metacritic-harvester/internal/domain"
	"gopkg.in/yaml.v3"
)

type BatchFile struct {
	Defaults BatchDefaults   `yaml:"defaults"`
	Tasks    []BatchTaskSpec `yaml:"tasks"`
}

type BatchDefaults struct {
	DBPath      string   `yaml:"db"`
	Pages       *int     `yaml:"pages"`
	Retries     *int     `yaml:"retries"`
	Debug       *bool    `yaml:"debug"`
	Proxies     []string `yaml:"proxies"`
	Concurrency *int     `yaml:"concurrency"`
}

type BatchTaskKind string

const (
	BatchTaskKindList   BatchTaskKind = "list"
	BatchTaskKindDetail BatchTaskKind = "detail"
	BatchTaskKindReview BatchTaskKind = "reviews"
)

type BatchTaskSpec struct {
	Name              string   `yaml:"name"`
	Kind              string   `yaml:"kind"`
	Source            string   `yaml:"source"`
	Category          string   `yaml:"category"`
	Metric            string   `yaml:"metric"`
	Year              string   `yaml:"year"`
	Platform          []string `yaml:"platform"`
	Network           []string `yaml:"network"`
	Genre             []string `yaml:"genre"`
	ReleaseType       []string `yaml:"release-type"`
	Pages             *int     `yaml:"pages"`
	WorkHref          string   `yaml:"work-href"`
	Limit             *int     `yaml:"limit"`
	Force             *bool    `yaml:"force"`
	DetailConcurrency *int     `yaml:"detail-concurrency"`
	ReviewType        string   `yaml:"review-type"`
	Sentiment         string   `yaml:"sentiment"`
	Sort              string   `yaml:"sort"`
	PageSize          *int     `yaml:"page-size"`
	MaxPages          *int     `yaml:"max-pages"`
	ReviewConcurrency *int     `yaml:"review-concurrency"`
	DBPath            string   `yaml:"db"`
	Retries           *int     `yaml:"retries"`
	Debug             *bool    `yaml:"debug"`
	Proxies           []string `yaml:"proxies"`
}

type BatchTaskConfig struct {
	Name   string
	Kind   BatchTaskKind
	List   *ListCommandConfig
	Detail *DetailCommandConfig
	Review *ReviewCommandConfig
}

type BatchRunConfig struct {
	Tasks       []BatchTaskConfig
	Concurrency int
}

func LoadBatchFile(path string) (BatchFile, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return BatchFile{}, fmt.Errorf("read batch file: %w", err)
	}

	var batchFile BatchFile
	if err := yaml.Unmarshal(content, &batchFile); err != nil {
		return BatchFile{}, fmt.Errorf("parse batch file: %w", err)
	}
	if len(batchFile.Tasks) == 0 {
		return BatchFile{}, fmt.Errorf("batch file must include at least one task")
	}

	return batchFile, nil
}

func BuildBatchTaskConfigs(file BatchFile) ([]BatchTaskConfig, error) {
	result := make([]BatchTaskConfig, 0, len(file.Tasks))

	for idx, task := range file.Tasks {
		kind, err := parseBatchTaskKind(task.Kind)
		if err != nil {
			return nil, fmt.Errorf("task %d: %w", idx+1, err)
		}

		switch kind {
		case BatchTaskKindList:
			options := ListCommandOptions{
				Category:    strings.TrimSpace(task.Category),
				Metric:      strings.TrimSpace(task.Metric),
				Source:      firstNonEmpty(task.Source, string(CrawlSourceAPI)),
				Year:        strings.TrimSpace(task.Year),
				Platform:    strings.Join(task.Platform, ","),
				Network:     strings.Join(task.Network, ","),
				Genre:       strings.Join(task.Genre, ","),
				ReleaseType: strings.Join(task.ReleaseType, ","),
				Pages:       firstPositivePointer(1, task.Pages, file.Defaults.Pages),
				DBPath:      firstNonEmpty(task.DBPath, file.Defaults.DBPath, "output/metacritic.db"),
				Debug:       mergeBool(false, task.Debug, file.Defaults.Debug),
				MaxRetries:  firstNonNegativePointer(3, task.Retries, file.Defaults.Retries),
				Proxies:     strings.Join(firstNonEmptySlice(task.Proxies, file.Defaults.Proxies), ","),
			}

			cfg, err := BuildListCommandConfig(options)
			if err != nil {
				return nil, fmt.Errorf("task %d: %w", idx+1, err)
			}

			name := strings.TrimSpace(task.Name)
			if name == "" {
				name = fmt.Sprintf("%s-%s-%d", cfg.Task.Category, cfg.Task.Metric, idx+1)
			}

			result = append(result, BatchTaskConfig{
				Name: name,
				Kind: kind,
				List: &cfg,
			})
		case BatchTaskKindDetail:
			if err := validateDetailBatchTask(task); err != nil {
				return nil, fmt.Errorf("task %d: %w", idx+1, err)
			}

			concurrency := firstPositivePointer(1, task.DetailConcurrency)
			options := DetailCommandOptions{
				Category:    strings.TrimSpace(task.Category),
				WorkHref:    strings.TrimSpace(task.WorkHref),
				Source:      firstNonEmpty(task.Source, string(CrawlSourceAPI)),
				Limit:       firstNonNegativePointer(0, task.Limit),
				Force:       mergeBool(false, task.Force),
				DBPath:      firstNonEmpty(task.DBPath, file.Defaults.DBPath, "output/metacritic.db"),
				Debug:       mergeBool(false, task.Debug, file.Defaults.Debug),
				MaxRetries:  firstNonNegativePointer(3, task.Retries, file.Defaults.Retries),
				Proxies:     strings.Join(firstNonEmptySlice(task.Proxies, file.Defaults.Proxies), ","),
				Concurrency: concurrency,
			}

			cfg, err := BuildDetailCommandConfig(options)
			if err != nil {
				return nil, fmt.Errorf("task %d: %w", idx+1, err)
			}

			name := strings.TrimSpace(task.Name)
			if name == "" {
				name = defaultDetailTaskName(cfg.Task, idx+1)
			}

			result = append(result, BatchTaskConfig{
				Name:   name,
				Kind:   kind,
				Detail: &cfg,
			})
		case BatchTaskKindReview:
			if err := validateReviewBatchTask(task); err != nil {
				return nil, fmt.Errorf("task %d: %w", idx+1, err)
			}

			concurrency := firstPositivePointer(1, task.ReviewConcurrency, file.Defaults.Concurrency)
			options := ReviewCommandOptions{
				Category:    strings.TrimSpace(task.Category),
				WorkHref:    strings.TrimSpace(task.WorkHref),
				Limit:       firstNonNegativePointer(0, task.Limit),
				Force:       mergeBool(false, task.Force),
				Concurrency: concurrency,
				ReviewType:  firstNonEmpty(task.ReviewType, string(domain.ReviewTypeAll)),
				Sentiment:   firstNonEmpty(task.Sentiment, string(domain.ReviewSentimentAll)),
				Sort:        strings.TrimSpace(task.Sort),
				Platform:    strings.Join(task.Platform, ","),
				PageSize:    firstPositivePointer(20, task.PageSize),
				MaxPages:    firstNonNegativePointer(0, task.MaxPages),
				DBPath:      firstNonEmpty(task.DBPath, file.Defaults.DBPath, "output/metacritic.db"),
				Debug:       mergeBool(false, task.Debug, file.Defaults.Debug),
				MaxRetries:  firstNonNegativePointer(3, task.Retries, file.Defaults.Retries),
				Proxies:     strings.Join(firstNonEmptySlice(task.Proxies, file.Defaults.Proxies), ","),
			}

			cfg, err := BuildReviewCommandConfig(options)
			if err != nil {
				return nil, fmt.Errorf("task %d: %w", idx+1, err)
			}

			name := strings.TrimSpace(task.Name)
			if name == "" {
				name = defaultReviewTaskName(cfg.Task, idx+1)
			}

			result = append(result, BatchTaskConfig{
				Name:   name,
				Kind:   kind,
				Review: &cfg,
			})
		}
	}

	return result, nil
}

func BuildBatchRunConfig(file BatchFile, overrideConcurrency int) (BatchRunConfig, error) {
	tasks, err := BuildBatchTaskConfigs(file)
	if err != nil {
		return BatchRunConfig{}, err
	}

	return BatchRunConfig{
		Tasks:       tasks,
		Concurrency: resolveBatchConcurrency(overrideConcurrency, file.Defaults.Concurrency),
	}, nil
}

func firstPositivePointer(defaultValue int, values ...*int) int {
	for _, value := range values {
		if value != nil && *value > 0 {
			return *value
		}
	}
	return defaultValue
}

func firstNonNegativePointer(defaultValue int, values ...*int) int {
	for _, value := range values {
		if value != nil && *value >= 0 {
			return *value
		}
	}
	return defaultValue
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstNonEmptySlice(values ...[]string) []string {
	for _, value := range values {
		if len(value) > 0 {
			return value
		}
	}
	return nil
}

func mergeBool(defaultValue bool, values ...*bool) bool {
	for _, value := range values {
		if value != nil {
			return *value
		}
	}
	return defaultValue
}

func resolveBatchConcurrency(override int, defaults ...*int) int {
	if override > 0 {
		return override
	}
	for _, value := range defaults {
		if value != nil && *value > 0 {
			return *value
		}
	}
	return 1
}

func parseBatchTaskKind(raw string) (BatchTaskKind, error) {
	switch strings.TrimSpace(raw) {
	case "", string(BatchTaskKindList):
		return BatchTaskKindList, nil
	case string(BatchTaskKindDetail):
		return BatchTaskKindDetail, nil
	case string(BatchTaskKindReview):
		return BatchTaskKindReview, nil
	default:
		return "", fmt.Errorf("invalid task kind %q", raw)
	}
}

func validateDetailBatchTask(task BatchTaskSpec) error {
	if strings.TrimSpace(task.Metric) != "" {
		return fmt.Errorf("detail task does not support metric")
	}
	if strings.TrimSpace(task.Year) != "" {
		return fmt.Errorf("detail task does not support year")
	}
	if len(task.Platform) > 0 {
		return fmt.Errorf("detail task does not support platform")
	}
	if len(task.Network) > 0 {
		return fmt.Errorf("detail task does not support network")
	}
	if len(task.Genre) > 0 {
		return fmt.Errorf("detail task does not support genre")
	}
	if len(task.ReleaseType) > 0 {
		return fmt.Errorf("detail task does not support release-type")
	}
	if task.Pages != nil {
		return fmt.Errorf("detail task does not support pages")
	}
	return nil
}

func defaultDetailTaskName(task domain.DetailTask, index int) string {
	if strings.TrimSpace(task.WorkHref) != "" {
		return fmt.Sprintf("detail-single-%d", index)
	}
	category := strings.TrimSpace(string(task.Category))
	if category == "" {
		category = "all"
	}
	return fmt.Sprintf("detail-%s-%d", category, index)
}

func validateReviewBatchTask(task BatchTaskSpec) error {
	if strings.TrimSpace(task.Metric) != "" {
		return fmt.Errorf("reviews task does not support metric")
	}
	if strings.TrimSpace(task.Year) != "" {
		return fmt.Errorf("reviews task does not support year")
	}
	if len(task.Network) > 0 {
		return fmt.Errorf("reviews task does not support network")
	}
	if len(task.Genre) > 0 {
		return fmt.Errorf("reviews task does not support genre")
	}
	if len(task.ReleaseType) > 0 {
		return fmt.Errorf("reviews task does not support release-type")
	}
	if task.Pages != nil {
		return fmt.Errorf("reviews task does not support pages")
	}
	if task.DetailConcurrency != nil {
		return fmt.Errorf("reviews task does not support detail-concurrency")
	}
	if strings.TrimSpace(task.Source) != "" {
		return fmt.Errorf("reviews task does not support source")
	}
	return nil
}

func defaultReviewTaskName(task domain.ReviewTask, index int) string {
	category := strings.TrimSpace(string(task.Category))
	if category == "" {
		category = "all"
	}
	reviewType := strings.TrimSpace(string(task.ReviewType))
	if reviewType == "" {
		reviewType = string(domain.ReviewTypeAll)
	}
	if strings.TrimSpace(task.WorkHref) != "" {
		return fmt.Sprintf("reviews-single-%d", index)
	}
	return fmt.Sprintf("reviews-%s-%s-%d", category, reviewType, index)
}
