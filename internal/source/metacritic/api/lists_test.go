package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GoFurry/metacritic-harvester/internal/domain"
)

func TestFinderAPIFetchPageFromFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category domain.Category
		metric   domain.Metric
		fixture  string
		wantHref string
		wantName string
		wantMeta string
		wantUser string
	}{
		{name: "game", category: domain.CategoryGame, metric: domain.MetricMetascore, fixture: "game.json", wantHref: "https://www.metacritic.com/game/baldurs-gate-3/", wantName: "Baldur's Gate 3", wantMeta: "96", wantUser: "8.9"},
		{name: "movie", category: domain.CategoryMovie, metric: domain.MetricUserScore, fixture: "movie.json", wantHref: "https://www.metacritic.com/movie/pk/", wantName: "PK", wantMeta: "77", wantUser: "8.3"},
		{name: "tv", category: domain.CategoryTV, metric: domain.MetricNewest, fixture: "tv.json", wantHref: "https://www.metacritic.com/tv/bojack-horseman/", wantName: "BoJack Horseman", wantMeta: "82", wantUser: "8.8"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var requestPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestPath = r.URL.String()
				_, _ = w.Write([]byte(readListFixture(t, tt.fixture)))
			}))
			defer server.Close()

			api := NewFinderAPI(server.URL, nil, 5*time.Second, 0)
			page, err := api.FetchPage(context.Background(), domain.ListTask{
				Category: tt.category,
				Metric:   tt.metric,
				Filter: domain.Filter{
					Genres: []string{"action"},
				},
				MaxPages: 1,
			}, 1)
			if err != nil {
				t.Fatalf("FetchPage() error = %v", err)
			}
			if page.LastPage < 1 || len(page.Items) != 1 {
				t.Fatalf("unexpected page: %+v", page)
			}
			item := page.Items[0]
			if item.Work.Href != tt.wantHref || item.Work.Name != tt.wantName || item.Metascore != tt.wantMeta || item.UserScore != tt.wantUser {
				t.Fatalf("unexpected item: %+v", item)
			}
			if !strings.Contains(requestPath, "/finder/metacritic/web?") {
				t.Fatalf("expected finder endpoint, got %q", requestPath)
			}
		})
	}
}

func TestBuildFinderListURLIncludesMappedFilters(t *testing.T) {
	t.Parallel()

	reqURL, err := BuildFinderListURLForTest("https://backend.metacritic.com", domain.ListTask{
		Category: domain.CategoryGame,
		Metric:   domain.MetricMetascore,
		Filter: domain.Filter{
			Platforms: []string{"pc", "ps5"},
			Genres:    []string{"western-rpg"},
		},
	}, 2)
	if err != nil {
		t.Fatalf("BuildFinderListURLForTest() error = %v", err)
	}
	if !strings.Contains(reqURL, "offset=24") || !strings.Contains(reqURL, "mcoTypeId=13") || !strings.Contains(reqURL, "gamePlatformIds=1500000019%2C1500000128") {
		t.Fatalf("unexpected finder URL: %s", reqURL)
	}
}

func readListFixture(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("testdata", "lists", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return string(content)
}
