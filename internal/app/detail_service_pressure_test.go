package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestDetailServicePressure(t *testing.T) {
	if testing.Short() || os.Getenv("DETAIL_PRESSURE") == "" {
		t.Skip("set DETAIL_PRESSURE=1 to run pressure coverage")
	}

	for _, totalWorks := range []int{100, 500} {
		t.Run(fmt.Sprintf("works_%d", totalWorks), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(detailServiceGameHTML("Pressure " + r.URL.Path)))
			}))
			defer server.Close()

			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("pressure-%d.db", totalWorks))
			works := make([]domain.Work, 0, totalWorks)
			for i := 0; i < totalWorks; i++ {
				works = append(works, domain.Work{
					Name:     fmt.Sprintf("Game %d", i),
					Href:     fmt.Sprintf("%s/game/%d", server.URL, i),
					Category: domain.CategoryGame,
				})
			}
			seedDetailServiceDB(t, ctx, dbPath, works)

			service := NewDetailService(DetailServiceConfig{
				BaseURL:    server.URL,
				DBPath:     dbPath,
				MaxRetries: 0,
			})
			service.sleep = func(time.Duration) {}

			result, err := service.Run(ctx, domain.DetailTask{
				Category:    domain.CategoryGame,
				Concurrency: 8,
			})
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if result.Total != totalWorks || result.Processed != totalWorks || result.Fetched != totalWorks || result.Failures != 0 {
				t.Fatalf("unexpected result: %+v", result)
			}
		})
	}
}
