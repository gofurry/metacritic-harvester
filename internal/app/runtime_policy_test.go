package app

import (
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/crawler"
	"golang.org/x/time/rate"
)

func TestApplyRuntimePolicyOverridePreservesConcurrencyDefaults(t *testing.T) {
	t.Parallel()

	base := detailRuntimePolicy(6)
	merged := applyRuntimePolicyOverride(base, &crawler.HTTPRuntimePolicy{
		RateLimit: rate.Limit(5.5),
		RateBurst: 8,
	})

	if merged.MaxInFlight != 6 {
		t.Fatalf("expected max in-flight 6, got %+v", merged)
	}
	if merged.RateLimit != rate.Limit(5.5) || merged.RateBurst != 8 {
		t.Fatalf("expected override rate settings, got %+v", merged)
	}
	if merged.Timeout != 30*time.Second {
		t.Fatalf("expected default timeout to stay unchanged, got %+v", merged)
	}
}

func TestListRuntimePolicyUsesConfiguredDefaults(t *testing.T) {
	t.Parallel()

	policy := listRuntimePolicy()
	if policy.RateLimit != rate.Limit(config.DefaultCrawlRateRPS) {
		t.Fatalf("expected default rps %v, got %+v", config.DefaultCrawlRateRPS, policy)
	}
	if policy.RateBurst != config.DefaultCrawlRateBurst {
		t.Fatalf("expected default burst %d, got %+v", config.DefaultCrawlRateBurst, policy)
	}
}
