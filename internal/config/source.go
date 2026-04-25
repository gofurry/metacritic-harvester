package config

import (
	"fmt"
	"strings"
)

type CrawlSource string

const (
	CrawlSourceAPI  CrawlSource = "api"
	CrawlSourceHTML CrawlSource = "html"
	CrawlSourceAuto CrawlSource = "auto"
)

func ParseCrawlSource(raw string) (CrawlSource, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", string(CrawlSourceAPI):
		return CrawlSourceAPI, nil
	case string(CrawlSourceHTML):
		return CrawlSourceHTML, nil
	case string(CrawlSourceAuto):
		return CrawlSourceAuto, nil
	default:
		return "", fmt.Errorf("invalid source %q: must be one of api|html|auto", raw)
	}
}
