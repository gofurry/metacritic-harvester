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

func TestComposerAPIFetchFromFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		work     domain.Work
		fixture  string
		assertFn func(t *testing.T, detail domain.WorkDetail)
	}{
		{
			name:    "game",
			work:    domain.Work{Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame},
			fixture: "game.json",
			assertFn: func(t *testing.T, detail domain.WorkDetail) {
				if detail.Title != "Baldur's Gate 3" || detail.Metascore != "96" || detail.Details.CurrentPlatform != "PC" || len(detail.Details.Platforms) != 1 {
					t.Fatalf("unexpected game detail: %+v", detail)
				}
			},
		},
		{
			name:    "movie",
			work:    domain.Work{Href: "https://www.metacritic.com/movie/pk/", Category: domain.CategoryMovie},
			fixture: "movie.json",
			assertFn: func(t *testing.T, detail domain.WorkDetail) {
				if detail.Title != "PK" || detail.Tagline != "Question the world" || len(detail.Details.Directors) != 1 || len(detail.Details.ProductionCompanies) != 1 {
					t.Fatalf("unexpected movie detail: %+v", detail)
				}
			},
		},
		{
			name:    "tv",
			work:    domain.Work{Href: "https://www.metacritic.com/tv/bojack-horseman/", Category: domain.CategoryTV},
			fixture: "tv.json",
			assertFn: func(t *testing.T, detail domain.WorkDetail) {
				if detail.Title != "BoJack Horseman" || detail.Details.NumberOfSeasons != "6" || len(detail.Details.Genres) != 2 {
					t.Fatalf("unexpected tv detail: %+v", detail)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(readDetailFixture(t, tt.fixture)))
			}))
			defer server.Close()

			api := NewComposerAPI(server.URL, nil, 5*time.Second, 0)
			detail, err := api.Fetch(context.Background(), tt.work)
			if err != nil {
				t.Fatalf("Fetch() error = %v", err)
			}
			tt.assertFn(t, detail)
		})
	}
}

func TestComposerAPIFetchReturnsMissingRequiredFieldsError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"components":[{"meta":{"componentName":"product"},"data":{"item":{"title":"","description":"no title"}}}]}`))
	}))
	defer server.Close()

	api := NewComposerAPI(server.URL, nil, 5*time.Second, 0)
	_, err := api.Fetch(context.Background(), domain.Work{
		Href:     "https://www.metacritic.com/movie/pk/",
		Category: domain.CategoryMovie,
	})
	if err == nil {
		t.Fatal("expected missing required fields error")
	}
	if !IsComposerMissingRequiredFieldsError(err) {
		t.Fatalf("expected composer missing required fields error, got %v", err)
	}
}

func readDetailFixture(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("testdata", "details", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return string(content)
}
