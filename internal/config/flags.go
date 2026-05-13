package config

import (
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func BuildListCommandConfig(opts ListCommandOptions) (ListCommandConfig, error) {
	category, err := domain.ParseCategory(opts.Category)
	if err != nil {
		return ListCommandConfig{}, err
	}

	metric, err := domain.ParseMetric(opts.Metric)
	if err != nil {
		return ListCommandConfig{}, err
	}

	source, err := ParseCrawlSource(opts.Source)
	if err != nil {
		return ListCommandConfig{}, err
	}

	if opts.Pages < 0 {
		return ListCommandConfig{}, fmt.Errorf("pages must be greater than or equal to 0")
	}
	if opts.Timeout < 0 {
		return ListCommandConfig{}, fmt.Errorf("timeout must not be negative")
	}
	if opts.RPS < 0 {
		return ListCommandConfig{}, fmt.Errorf("rps must not be negative")
	}
	if opts.Burst < 0 {
		return ListCommandConfig{}, fmt.Errorf("burst must not be negative")
	}
	if opts.MaxRetries < 0 {
		return ListCommandConfig{}, fmt.Errorf("retries must be greater than or equal to 0")
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultCrawlCommandTimeout
	}
	rps := opts.RPS
	if rps == 0 {
		rps = DefaultCrawlRateRPS
	}
	burst := opts.Burst
	if burst == 0 {
		burst = DefaultCrawlRateBurst
	}

	filter, err := buildFilter(category, opts)
	if err != nil {
		return ListCommandConfig{}, err
	}

	proxies, err := SplitAndValidateProxyList(opts.Proxies)
	if err != nil {
		return ListCommandConfig{}, err
	}

	dbPath := strings.TrimSpace(opts.DBPath)
	if dbPath == "" {
		return ListCommandConfig{}, fmt.Errorf("db path must not be empty")
	}

	return ListCommandConfig{
		Task: domain.ListTask{
			Category: category,
			Metric:   metric,
			Filter:   filter,
			MaxPages: opts.Pages,
			Debug:    opts.Debug,
		},
		Source:          source,
		DBPath:          filepath.Clean(dbPath),
		Debug:           opts.Debug,
		Timeout:         timeout,
		ContinueOnError: opts.ContinueOnError,
		RPS:             rps,
		Burst:           burst,
		MaxRetries:      opts.MaxRetries,
		ProxyURLs:       proxies,
	}, nil
}

func BuildDetailCommandConfig(opts DetailCommandOptions) (DetailCommandConfig, error) {
	var category domain.Category
	var err error
	if strings.TrimSpace(opts.Category) != "" {
		category, err = domain.ParseCategory(opts.Category)
		if err != nil {
			return DetailCommandConfig{}, err
		}
	}

	if opts.Limit < 0 {
		return DetailCommandConfig{}, fmt.Errorf("limit must be greater than or equal to 0")
	}
	if opts.Concurrency <= 0 {
		return DetailCommandConfig{}, fmt.Errorf("concurrency must be greater than 0")
	}
	if opts.Timeout < 0 {
		return DetailCommandConfig{}, fmt.Errorf("timeout must not be negative")
	}
	if opts.RPS < 0 {
		return DetailCommandConfig{}, fmt.Errorf("rps must not be negative")
	}
	if opts.Burst < 0 {
		return DetailCommandConfig{}, fmt.Errorf("burst must not be negative")
	}
	if opts.MaxRetries < 0 {
		return DetailCommandConfig{}, fmt.Errorf("retries must be greater than or equal to 0")
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultCrawlCommandTimeout
	}
	rps := opts.RPS
	if rps == 0 {
		rps = DefaultCrawlRateRPS
	}
	burst := opts.Burst
	if burst == 0 {
		burst = DefaultCrawlRateBurst
	}

	source, err := ParseCrawlSource(opts.Source)
	if err != nil {
		return DetailCommandConfig{}, err
	}

	proxies, err := SplitAndValidateProxyList(opts.Proxies)
	if err != nil {
		return DetailCommandConfig{}, err
	}

	dbPath := strings.TrimSpace(opts.DBPath)
	if dbPath == "" {
		return DetailCommandConfig{}, fmt.Errorf("db path must not be empty")
	}

	workHref := domain.NormalizeWorkHref(opts.WorkHref, DefaultBaseURL)

	return DetailCommandConfig{
		Task: domain.DetailTask{
			Category:    category,
			WorkHref:    workHref,
			Limit:       opts.Limit,
			Force:       opts.Force,
			Debug:       opts.Debug,
			Concurrency: opts.Concurrency,
		},
		Source:          source,
		DBPath:          filepath.Clean(dbPath),
		Debug:           opts.Debug,
		Timeout:         timeout,
		ContinueOnError: opts.ContinueOnError,
		RPS:             rps,
		Burst:           burst,
		MaxRetries:      opts.MaxRetries,
		ProxyURLs:       proxies,
		Concurrency:     opts.Concurrency,
	}, nil
}

func BuildReviewCommandConfig(opts ReviewCommandOptions) (ReviewCommandConfig, error) {
	var (
		category domain.Category
		err      error
	)
	if strings.TrimSpace(opts.Category) != "" {
		category, err = domain.ParseCategory(opts.Category)
		if err != nil {
			return ReviewCommandConfig{}, err
		}
	}

	reviewType, err := domain.ParseReviewType(opts.ReviewType)
	if err != nil {
		return ReviewCommandConfig{}, err
	}
	sentiment, err := domain.ParseReviewSentiment(opts.Sentiment)
	if err != nil {
		return ReviewCommandConfig{}, err
	}
	sort, err := domain.ParseReviewSort(opts.Sort)
	if err != nil {
		return ReviewCommandConfig{}, err
	}

	if opts.Limit < 0 {
		return ReviewCommandConfig{}, fmt.Errorf("limit must be greater than or equal to 0")
	}
	if opts.Concurrency <= 0 {
		return ReviewCommandConfig{}, fmt.Errorf("concurrency must be greater than 0")
	}
	if opts.PageSize <= 0 {
		return ReviewCommandConfig{}, fmt.Errorf("page-size must be greater than 0")
	}
	if opts.MaxPages < 0 {
		return ReviewCommandConfig{}, fmt.Errorf("max-pages must be greater than or equal to 0")
	}
	if opts.Timeout < 0 {
		return ReviewCommandConfig{}, fmt.Errorf("timeout must not be negative")
	}
	if opts.RPS < 0 {
		return ReviewCommandConfig{}, fmt.Errorf("rps must not be negative")
	}
	if opts.Burst < 0 {
		return ReviewCommandConfig{}, fmt.Errorf("burst must not be negative")
	}
	if opts.MaxRetries < 0 {
		return ReviewCommandConfig{}, fmt.Errorf("retries must be greater than or equal to 0")
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultCrawlCommandTimeout
	}
	rps := opts.RPS
	if rps == 0 {
		rps = DefaultCrawlRateRPS
	}
	burst := opts.Burst
	if burst == 0 {
		burst = DefaultCrawlRateBurst
	}

	platform := strings.TrimSpace(opts.Platform)
	if platform != "" && category != domain.CategoryGame {
		return ReviewCommandConfig{}, fmt.Errorf("platform is only supported for category game")
	}

	proxies, err := SplitAndValidateProxyList(opts.Proxies)
	if err != nil {
		return ReviewCommandConfig{}, err
	}

	dbPath := strings.TrimSpace(opts.DBPath)
	if dbPath == "" {
		return ReviewCommandConfig{}, fmt.Errorf("db path must not be empty")
	}

	workHref := domain.NormalizeWorkHref(opts.WorkHref, DefaultBaseURL)

	return ReviewCommandConfig{
		Task: domain.ReviewTask{
			Category:    category,
			WorkHref:    workHref,
			Limit:       opts.Limit,
			Force:       opts.Force,
			Concurrency: opts.Concurrency,
			ReviewType:  reviewType,
			Sentiment:   sentiment,
			Sort:        sort,
			Platform:    platform,
			PageSize:    opts.PageSize,
			MaxPages:    opts.MaxPages,
			Debug:       opts.Debug,
		},
		DBPath:          filepath.Clean(dbPath),
		Debug:           opts.Debug,
		Timeout:         timeout,
		ContinueOnError: opts.ContinueOnError,
		RPS:             rps,
		Burst:           burst,
		MaxRetries:      opts.MaxRetries,
		ProxyURLs:       proxies,
		Concurrency:     opts.Concurrency,
	}, nil
}

func SplitAndValidateProxyList(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		parsed, err := url.Parse(part)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("invalid proxy url %q", part)
		}

		result = append(result, part)
	}
	return result, nil
}

var yearRangeRe = regexp.MustCompile(`^\d{4}:\d{4}$`)

func buildFilter(category domain.Category, opts ListCommandOptions) (domain.Filter, error) {
	filter := domain.Filter{
		Platforms:    splitCSV(opts.Platform),
		Networks:     splitCSV(opts.Network),
		Genres:       splitCSV(opts.Genre),
		ReleaseTypes: splitCSV(opts.ReleaseType),
	}

	if opts.Year != "" {
		minYear, maxYear, err := parseYearRange(opts.Year)
		if err != nil {
			return domain.Filter{}, err
		}
		filter.ReleaseYearMin = &minYear
		filter.ReleaseYearMax = &maxYear
	}

	switch category {
	case domain.CategoryGame:
		if len(filter.Networks) > 0 {
			return domain.Filter{}, fmt.Errorf("network is not supported for category game")
		}
	case domain.CategoryMovie:
		if len(filter.Platforms) > 0 {
			return domain.Filter{}, fmt.Errorf("platform is not supported for category movie; use --network")
		}
	case domain.CategoryTV:
		if len(filter.Platforms) > 0 {
			return domain.Filter{}, fmt.Errorf("platform is not supported for category tv; use --network")
		}
		if len(filter.ReleaseTypes) > 0 {
			return domain.Filter{}, fmt.Errorf("release-type is not supported for category tv")
		}
	}

	return filter, nil
}

func parseYearRange(raw string) (int, int, error) {
	raw = strings.TrimSpace(raw)
	if !yearRangeRe.MatchString(raw) {
		return 0, 0, fmt.Errorf("year must be in YYYY:YYYY format")
	}

	parts := strings.Split(raw, ":")
	minYear, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid year range %q", raw)
	}
	maxYear, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid year range %q", raw)
	}
	if minYear > maxYear {
		return 0, 0, fmt.Errorf("year range must satisfy min <= max")
	}
	return minYear, maxYear, nil
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		result = append(result, part)
	}
	return result
}
