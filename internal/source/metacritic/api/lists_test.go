package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
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

func TestBuildFinderListURLNormalizesFinderMappings(t *testing.T) {
	t.Parallel()

	reqURL, err := BuildFinderListURLForTest("https://backend.metacritic.com", domain.ListTask{
		Category: domain.CategoryGame,
		Metric:   domain.MetricMetascore,
		Filter: domain.Filter{
			Platforms: []string{"PlayStation 5", "Xbox Series X|S"},
			Networks:  []string{"Netflix"},
			Genres:    []string{"western-rpg"},
		},
	}, 1)
	if err != nil {
		t.Fatalf("BuildFinderListURLForTest() error = %v", err)
	}

	if !strings.Contains(reqURL, "gamePlatformIds=1500000128%2C1500000129") {
		t.Fatalf("expected normalized platform ids in %q", reqURL)
	}
	if !strings.Contains(reqURL, "streamingNetworkIds=1943") {
		t.Fatalf("expected normalized network ids in %q", reqURL)
	}
	if !strings.Contains(reqURL, "genres=Western+RPG") {
		t.Fatalf("expected normalized genres in %q", reqURL)
	}
}

func TestBuildFinderListURLReturnsStableMappingErrors(t *testing.T) {
	t.Parallel()

	_, err := BuildFinderListURLForTest("https://backend.metacritic.com", domain.ListTask{
		Category: domain.CategoryGame,
		Metric:   domain.MetricMetascore,
		Filter: domain.Filter{
			Platforms: []string{"mystery-box"},
		},
	}, 1)
	if err == nil {
		t.Fatal("expected mapping error")
	}
	if !IsFinderMappingError(err) {
		t.Fatalf("expected finder mapping error, got %v", err)
	}
	var target *finderMappingError
	if !errors.As(err, &target) || target.Kind != finderMappingKindPlatform {
		t.Fatalf("expected platform mapping error, got %T %v", err, err)
	}
}

func TestFinderAPIFetchPageMissingRequiredFields(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"items":[{"title":"","slug":""}]}}`))
	}))
	defer server.Close()

	api := NewFinderAPI(server.URL, nil, 5*time.Second, 0)
	_, err := api.FetchPage(context.Background(), domain.ListTask{
		Category: domain.CategoryGame,
		Metric:   domain.MetricMetascore,
		MaxPages: 1,
	}, 1)
	if err == nil {
		t.Fatal("expected missing required fields error")
	}
	if !IsFinderMissingRequiredFieldsError(err) {
		t.Fatalf("expected missing required fields error, got %v", err)
	}
}

func TestFinderAPIFetchPageReturnsParseErrorForNonJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not-json"))
	}))
	defer server.Close()

	api := NewFinderAPI(server.URL, nil, 5*time.Second, 0)
	if _, err := api.FetchPage(context.Background(), domain.ListTask{
		Category: domain.CategoryGame,
		Metric:   domain.MetricMetascore,
		MaxPages: 1,
	}, 1); err == nil || !strings.Contains(err.Error(), "decode finder response") {
		t.Fatalf("expected decode finder response error, got %v", err)
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
