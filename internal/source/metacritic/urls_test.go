package metacritic

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestBuildListURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category domain.Category
		metric   domain.Metric
		page     int
		want     string
	}{
		{"game-metascore-page1", "game", "metascore", 1, "https://www.metacritic.com/browse/game/"},
		{"movie-metascore-page1", "movie", "metascore", 1, "https://www.metacritic.com/browse/movie/"},
		{"tv-metascore-page1", "tv", "metascore", 1, "https://www.metacritic.com/browse/tv/"},
		{"game-userscore-page1", "game", "userscore", 1, "https://www.metacritic.com/browse/game/all/all/all-time/userscore/"},
		{"movie-userscore-page1", "movie", "userscore", 1, "https://www.metacritic.com/browse/movie/all/all/all-time/userscore/"},
		{"tv-userscore-page1", "tv", "userscore", 1, "https://www.metacritic.com/browse/tv/all/all/all-time/userscore/"},
		{"game-newest-page1", "game", "newest", 1, "https://www.metacritic.com/browse/game/all/all/all-time/new/"},
		{"movie-newest-page1", "movie", "newest", 1, "https://www.metacritic.com/browse/movie/all/all/all-time/new/"},
		{"tv-newest-page2", "tv", "newest", 2, "https://www.metacritic.com/browse/tv/all/all/all-time/new/?page=2"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := BuildListURL(tt.category, tt.metric, domain.Filter{}, tt.page)
			if got != tt.want {
				t.Fatalf("BuildListURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildListURLWithFilters(t *testing.T) {
	t.Parallel()

	minYear := 2011
	maxYear := 2014
	tests := []struct {
		name     string
		category domain.Category
		metric   domain.Metric
		filter   domain.Filter
		page     int
		wantPath string
		want     map[string][]string
	}{
		{
			name:     "game-filters",
			category: "game",
			metric:   "metascore",
			page:     2,
			filter: domain.Filter{
				ReleaseYearMin: &minYear,
				ReleaseYearMax: &maxYear,
				Platforms:      []string{"pc", "ps5"},
				Genres:         []string{"action", "rpg"},
				ReleaseTypes:   []string{"coming-soon"},
			},
			wantPath: "/browse/game/",
			want: map[string][]string{
				"releaseYearMin": {"2011"},
				"releaseYearMax": {"2014"},
				"platform":       {"pc", "ps5"},
				"genre":          {"action", "rpg"},
				"releaseType":    {"coming-soon"},
				"page":           {"2"},
			},
		},
		{
			name:     "movie-filters",
			category: "movie",
			metric:   "userscore",
			page:     1,
			filter: domain.Filter{
				ReleaseYearMin: &minYear,
				ReleaseYearMax: &maxYear,
				Networks:       []string{"netflix", "max"},
				Genres:         []string{"drama", "thriller"},
				ReleaseTypes:   []string{"coming-soon", "in-theaters"},
			},
			wantPath: "/browse/movie/all/all/all-time/userscore/",
			want: map[string][]string{
				"releaseYearMin": {"2011"},
				"releaseYearMax": {"2014"},
				"network":        {"netflix", "max"},
				"genre":          {"drama", "thriller"},
				"releaseType":    {"coming-soon", "in-theaters"},
			},
		},
		{
			name:     "tv-filters",
			category: "tv",
			metric:   "newest",
			page:     3,
			filter: domain.Filter{
				ReleaseYearMin: &minYear,
				ReleaseYearMax: &maxYear,
				Networks:       []string{"hulu", "netflix"},
				Genres:         []string{"drama", "thriller"},
			},
			wantPath: "/browse/tv/all/all/all-time/new/",
			want: map[string][]string{
				"releaseYearMin": {"2011"},
				"releaseYearMax": {"2014"},
				"network":        {"hulu", "netflix"},
				"genre":          {"drama", "thriller"},
				"page":           {"3"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			raw := BuildListURL(tt.category, tt.metric, tt.filter, tt.page)
			parsed, err := url.Parse(raw)
			if err != nil {
				t.Fatalf("url.Parse() error = %v", err)
			}
			if parsed.Path != tt.wantPath {
				t.Fatalf("path = %q, want %q", parsed.Path, tt.wantPath)
			}

			query := parsed.Query()
			for key, wantValues := range tt.want {
				gotValues := query[key]
				if !reflect.DeepEqual(gotValues, wantValues) {
					t.Fatalf("query[%q] = %v, want %v", key, gotValues, wantValues)
				}
			}
		})
	}
}
