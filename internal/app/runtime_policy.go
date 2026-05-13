package app

import (
	"time"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/crawler"
	"golang.org/x/time/rate"
)

const (
	defaultHTTPTimeout = 30 * time.Second
)

func listRuntimePolicy() crawler.HTTPRuntimePolicy {
	return crawler.HTTPRuntimePolicy{
		Timeout:     defaultHTTPTimeout,
		RateLimit:   rate.Limit(config.DefaultCrawlRateRPS),
		RateBurst:   config.DefaultCrawlRateBurst,
		MaxInFlight: 1,
	}
}

func detailRuntimePolicy(concurrency int) crawler.HTTPRuntimePolicy {
	if concurrency <= 0 {
		concurrency = 1
	}
	return crawler.HTTPRuntimePolicy{
		Timeout:     defaultHTTPTimeout,
		RateLimit:   rate.Limit(config.DefaultCrawlRateRPS),
		RateBurst:   config.DefaultCrawlRateBurst,
		MaxInFlight: concurrency,
	}
}

func reviewRuntimePolicy(concurrency int) crawler.HTTPRuntimePolicy {
	if concurrency <= 0 {
		concurrency = 1
	}
	return crawler.HTTPRuntimePolicy{
		Timeout:     defaultHTTPTimeout,
		RateLimit:   rate.Limit(config.DefaultCrawlRateRPS),
		RateBurst:   config.DefaultCrawlRateBurst,
		MaxInFlight: concurrency,
	}
}

func applyRuntimePolicyOverride(base crawler.HTTPRuntimePolicy, override *crawler.HTTPRuntimePolicy) crawler.HTTPRuntimePolicy {
	if override == nil {
		return base
	}
	if override.Timeout > 0 {
		base.Timeout = override.Timeout
	}
	if override.RateLimit > 0 {
		base.RateLimit = override.RateLimit
	}
	if override.RateBurst > 0 {
		base.RateBurst = override.RateBurst
	}
	if override.MaxInFlight > 0 {
		base.MaxInFlight = override.MaxInFlight
	}
	return base
}
