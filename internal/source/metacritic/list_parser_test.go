package metacritic

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gocolly/colly/v2"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestParseListItemFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		fixture       string
		task          domain.ListTask
		wantName      string
		wantHref      string
		wantMetascore string
		wantUserScore string
		wantPageCount int
	}{
		{
			name:          "game-metascore",
			fixture:       "game_metascore.html",
			task:          domain.ListTask{Category: "game", Metric: "metascore"},
			wantName:      "Test Game",
			wantHref:      "https://www.metacritic.com/game/test-game",
			wantMetascore: "95",
			wantUserScore: "8.7",
			wantPageCount: 3,
		},
		{
			name:          "movie-userscore",
			fixture:       "movie_userscore.html",
			task:          domain.ListTask{Category: "movie", Metric: "userscore"},
			wantName:      "Test Movie",
			wantHref:      "https://www.metacritic.com/movie/test-movie",
			wantMetascore: "84",
			wantUserScore: "9.1",
			wantPageCount: 4,
		},
		{
			name:          "tv-newest",
			fixture:       "tv_newest.html",
			task:          domain.ListTask{Category: "tv", Metric: "newest"},
			wantName:      "Test Show",
			wantHref:      "https://www.metacritic.com/tv/test-show",
			wantPageCount: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body, err := os.ReadFile(filepath.Join("testdata", tt.fixture))
			if err != nil {
				t.Fatalf("ReadFile() error = %v", err)
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(body)
			}))
			defer server.Close()

			var parsedEntry domain.ListEntry
			var parsedWork domain.Work
			maxPage := 1

			c := colly.NewCollector()
			c.OnHTML(SelectorCard, func(e *colly.HTMLElement) {
				entry, work, ok := ParseListItem(e, 2, 3, tt.task)
				if !ok {
					t.Fatal("ParseListItem() returned ok=false")
				}
				parsedEntry = entry
				parsedWork = work
			})
			c.OnHTML(SelectorPagination, func(e *colly.HTMLElement) {
				maxPage = ParsePagination(e)
			})

			if err := c.Visit(server.URL); err != nil {
				t.Fatalf("Visit() error = %v", err)
			}

			if parsedWork.Name != tt.wantName {
				t.Fatalf("work name = %q, want %q", parsedWork.Name, tt.wantName)
			}
			if parsedWork.Href != tt.wantHref {
				t.Fatalf("work href = %q, want %q", parsedWork.Href, tt.wantHref)
			}
			if parsedEntry.Metascore != tt.wantMetascore {
				t.Fatalf("metascore = %q, want %q", parsedEntry.Metascore, tt.wantMetascore)
			}
			if parsedEntry.UserScore != tt.wantUserScore {
				t.Fatalf("user score = %q, want %q", parsedEntry.UserScore, tt.wantUserScore)
			}
			if parsedEntry.Page != 2 || parsedEntry.Rank != 3 {
				t.Fatalf("unexpected paging info: %+v", parsedEntry)
			}
			if maxPage != tt.wantPageCount {
				t.Fatalf("ParsePagination() = %d, want %d", maxPage, tt.wantPageCount)
			}
		})
	}
}
