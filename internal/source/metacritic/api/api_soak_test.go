package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/crawler"
	"github.com/gofurry/metacritic-harvester/internal/domain"
	"golang.org/x/time/rate"
)

func TestFinderAPISoak(t *testing.T) {
	if os.Getenv("METACRITIC_SOAK") != "1" {
		t.Skip("set METACRITIC_SOAK=1 to run finder soak tests")
	}

	var active int32
	var maxSeen int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&active, 1)
		defer atomic.AddInt32(&active, -1)
		updateMaxSeen(&maxSeen, current)
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte(readListFixture(t, "game.json")))
	}))
	defer server.Close()

	transport := crawler.WrapTransportWithPolicy(nil, crawler.HTTPRuntimePolicy{
		Timeout:     30 * time.Second,
		RateLimit:   rate.Limit(10),
		RateBurst:   2,
		MaxInFlight: 2,
	})
	api := NewFinderAPI(server.URL, transport, 30*time.Second, 0)
	task := domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore, MaxPages: 1}

	runConcurrentFetches(t, 20, func() error {
		_, err := api.FetchPage(context.Background(), task, 1)
		return err
	})

	if got := atomic.LoadInt32(&maxSeen); got > 2 {
		t.Fatalf("expected finder soak max in-flight <= 2, got %d", got)
	}
}

func TestComposerAPISoak(t *testing.T) {
	if os.Getenv("METACRITIC_SOAK") != "1" {
		t.Skip("set METACRITIC_SOAK=1 to run composer soak tests")
	}

	var active int32
	var maxSeen int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&active, 1)
		defer atomic.AddInt32(&active, -1)
		updateMaxSeen(&maxSeen, current)
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte(readDetailFixture(t, "game.json")))
	}))
	defer server.Close()

	transport := crawler.WrapTransportWithPolicy(nil, crawler.HTTPRuntimePolicy{
		Timeout:     30 * time.Second,
		RateLimit:   rate.Limit(10),
		RateBurst:   2,
		MaxInFlight: 2,
	})
	api := NewComposerAPI(server.URL, transport, 30*time.Second, 0)
	work := domain.Work{Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame}

	runConcurrentFetches(t, 20, func() error {
		_, err := api.Fetch(context.Background(), work)
		return err
	})

	if got := atomic.LoadInt32(&maxSeen); got > 2 {
		t.Fatalf("expected composer soak max in-flight <= 2, got %d", got)
	}
}

func TestReviewListAPISoak(t *testing.T) {
	if os.Getenv("METACRITIC_SOAK") != "1" {
		t.Skip("set METACRITIC_SOAK=1 to run review soak tests")
	}

	var active int32
	var maxSeen int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt32(&active, 1)
		defer atomic.AddInt32(&active, -1)
		updateMaxSeen(&maxSeen, current)
		time.Sleep(10 * time.Millisecond)
		_, _ = w.Write([]byte(readReviewFixture(t, "list_critic.json")))
	}))
	defer server.Close()

	transport := crawler.WrapTransportWithPolicy(nil, crawler.HTTPRuntimePolicy{
		Timeout:     30 * time.Second,
		RateLimit:   rate.Limit(10),
		RateBurst:   2,
		MaxInFlight: 2,
	})
	api := NewReviewListAPI(server.URL, transport, 30*time.Second, 0)
	work := domain.Work{Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame}

	runConcurrentFetches(t, 20, func() error {
		_, err := api.FetchPage(context.Background(), work, domain.ReviewTypeCritic, "", "", "pc", 0, 20)
		return err
	})

	if got := atomic.LoadInt32(&maxSeen); got > 2 {
		t.Fatalf("expected review soak max in-flight <= 2, got %d", got)
	}
}

func runConcurrentFetches(t *testing.T, calls int, fn func() error) {
	t.Helper()

	var wg sync.WaitGroup
	errCh := make(chan error, calls)
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- fn()
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("unexpected soak error: %v", err)
		}
	}
}

func updateMaxSeen(maxSeen *int32, current int32) {
	for {
		prev := atomic.LoadInt32(maxSeen)
		if current <= prev || atomic.CompareAndSwapInt32(maxSeen, prev, current) {
			return
		}
	}
}
