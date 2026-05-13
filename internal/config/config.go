package config

import (
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

const DefaultBaseURL = "https://www.metacritic.com"
const DefaultBackendBaseURL = "https://backend.metacritic.com"
const DefaultCrawlCommandTimeout = 3 * time.Hour
const DefaultCrawlRateRPS = 2.0
const DefaultCrawlRateBurst = 2

type ListCommandOptions struct {
	Category        string
	Metric          string
	Source          string
	Year            string
	Platform        string
	Network         string
	Genre           string
	ReleaseType     string
	Pages           int
	DBPath          string
	Debug           bool
	Timeout         time.Duration
	ContinueOnError bool
	RPS             float64
	Burst           int
	MaxRetries      int
	Proxies         string
}

type ListCommandConfig struct {
	Task            domain.ListTask
	Source          CrawlSource
	DBPath          string
	Debug           bool
	Timeout         time.Duration
	ContinueOnError bool
	RPS             float64
	Burst           int
	MaxRetries      int
	ProxyURLs       []string
}

type DetailCommandOptions struct {
	Category        string
	WorkHref        string
	Source          string
	Limit           int
	Force           bool
	DBPath          string
	Debug           bool
	Timeout         time.Duration
	ContinueOnError bool
	RPS             float64
	Burst           int
	MaxRetries      int
	Proxies         string
	Concurrency     int
}

type DetailCommandConfig struct {
	Task            domain.DetailTask
	Source          CrawlSource
	DBPath          string
	Debug           bool
	Timeout         time.Duration
	ContinueOnError bool
	RPS             float64
	Burst           int
	MaxRetries      int
	ProxyURLs       []string
	Concurrency     int
}

type ReviewCommandOptions struct {
	Category        string
	WorkHref        string
	Limit           int
	Force           bool
	Concurrency     int
	ReviewType      string
	Sentiment       string
	Sort            string
	Platform        string
	PageSize        int
	MaxPages        int
	DBPath          string
	Debug           bool
	Timeout         time.Duration
	ContinueOnError bool
	RPS             float64
	Burst           int
	MaxRetries      int
	Proxies         string
}

type ReviewCommandConfig struct {
	Task            domain.ReviewTask
	DBPath          string
	Debug           bool
	Timeout         time.Duration
	ContinueOnError bool
	RPS             float64
	Burst           int
	MaxRetries      int
	ProxyURLs       []string
	Concurrency     int
}
