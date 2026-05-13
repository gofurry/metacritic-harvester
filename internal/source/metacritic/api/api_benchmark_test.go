package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func BenchmarkFinderAPIFetchPage(b *testing.B) {
	if os.Getenv("METACRITIC_BENCH") != "1" {
		b.Skip("set METACRITIC_BENCH=1 to run API benchmarks")
	}
	fixture, err := os.ReadFile(filepath.Join("testdata", "lists", "game.json"))
	if err != nil {
		b.Fatalf("ReadFile() error = %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(fixture)
	}))
	defer server.Close()

	api := NewFinderAPI(server.URL, nil, 5*time.Second, 0)
	task := domain.ListTask{Category: domain.CategoryGame, Metric: domain.MetricMetascore, MaxPages: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := api.FetchPage(context.Background(), task, 1); err != nil {
			b.Fatalf("FetchPage() error = %v", err)
		}
	}
}

func BenchmarkComposerAPIFetch(b *testing.B) {
	if os.Getenv("METACRITIC_BENCH") != "1" {
		b.Skip("set METACRITIC_BENCH=1 to run API benchmarks")
	}
	fixture, err := os.ReadFile(filepath.Join("testdata", "details", "game.json"))
	if err != nil {
		b.Fatalf("ReadFile() error = %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(fixture)
	}))
	defer server.Close()

	api := NewComposerAPI(server.URL, nil, 5*time.Second, 0)
	work := domain.Work{Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := api.Fetch(context.Background(), work); err != nil {
			b.Fatalf("Fetch() error = %v", err)
		}
	}
}
