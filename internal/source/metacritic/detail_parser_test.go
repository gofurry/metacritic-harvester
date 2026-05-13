package metacritic

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestParseGameDetail(t *testing.T) {
	t.Parallel()

	detail := parseDetailFixture(t, domain.CategoryGame, "https://www.metacritic.com/game/baldurs-gate-3", "detail_game_ok.html")

	if detail.Title != "Baldur's Gate 3" {
		t.Fatalf("title = %q", detail.Title)
	}
	if detail.ReleaseDate != "Aug 3, 2023" {
		t.Fatalf("release date = %q", detail.ReleaseDate)
	}
	if detail.Summary != "Gather your party and return to the Forgotten Realms." {
		t.Fatalf("summary = %q", detail.Summary)
	}
	if detail.Rating != "Rated M" || detail.Details.ESRBDescription != "for Mature" {
		t.Fatalf("unexpected esrb fields: rating=%q desc=%q", detail.Rating, detail.Details.ESRBDescription)
	}
	if len(detail.Details.PlatformScores) != 2 {
		t.Fatalf("expected 2 platform scores, got %d", len(detail.Details.PlatformScores))
	}
	if detail.Details.PlatformScores[0].Platform != "PC" || detail.Details.PlatformScores[0].CriticReviewCount != 119 {
		t.Fatalf("unexpected platform score: %+v", detail.Details.PlatformScores[0])
	}
	if len(detail.Details.Developers) != 1 || detail.Details.Developers[0] != "Larian Studios Games" {
		t.Fatalf("developers = %+v", detail.Details.Developers)
	}
}

func TestParseMovieDetail(t *testing.T) {
	t.Parallel()

	detail := parseDetailFixture(t, domain.CategoryMovie, "https://www.metacritic.com/movie/the-leopard-re-release", "detail_movie_ok.html")

	if detail.Title != "The Leopard" {
		t.Fatalf("title = %q", detail.Title)
	}
	if detail.Metascore != "100" || detail.MetascoreReviewCount != 12 {
		t.Fatalf("unexpected metascore fields: score=%q count=%d", detail.Metascore, detail.MetascoreReviewCount)
	}
	if detail.UserScore != "7.7" || detail.UserScoreCount != 130 {
		t.Fatalf("unexpected user score fields: score=%q count=%d", detail.UserScore, detail.UserScoreCount)
	}
	if detail.Duration != "3 h 7 m" || detail.Rating != "PG" || detail.Tagline == "" {
		t.Fatalf("unexpected details: duration=%q rating=%q tagline=%q", detail.Duration, detail.Rating, detail.Tagline)
	}
	if len(detail.Details.Directors) != 1 || detail.Details.Directors[0] != "Luchino Visconti" {
		t.Fatalf("directors = %+v", detail.Details.Directors)
	}
	if len(detail.Details.Awards) != 1 || detail.Details.Awards[0].Event != "Academy Awards, USA" {
		t.Fatalf("awards = %+v", detail.Details.Awards)
	}
}

func TestParseTVDetail(t *testing.T) {
	t.Parallel()

	detail := parseDetailFixture(t, domain.CategoryTV, "https://www.metacritic.com/tv/the-office-uk", "detail_tv_ok.html")

	if detail.Title != "The Office" {
		t.Fatalf("title = %q", detail.Title)
	}
	if detail.Metascore != "97" || detail.UserScore != "8.2" {
		t.Fatalf("unexpected scores: metascore=%q user=%q", detail.Metascore, detail.UserScore)
	}
	if detail.Details.NumberOfSeasons != "3 Seasons" {
		t.Fatalf("number of seasons = %q", detail.Details.NumberOfSeasons)
	}
	if len(detail.Details.Seasons) != 2 {
		t.Fatalf("expected 2 seasons, got %d", len(detail.Details.Seasons))
	}
	if detail.Details.Seasons[0].Label != "Season 1" || detail.Details.Seasons[0].Episodes != "6 Episodes" || detail.Details.Seasons[0].Year != "2001" {
		t.Fatalf("unexpected season: %+v", detail.Details.Seasons[0])
	}
}

func TestParseDetailVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category domain.Category
		href     string
		fixture  string
		assert   func(t *testing.T, detail domain.WorkDetail)
	}{
		{
			name:     "game missing optional fields",
			category: domain.CategoryGame,
			href:     "https://www.metacritic.com/game/optional",
			fixture:  "detail_game_missing_optional.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if detail.Rating != "" || len(detail.Details.Developers) != 0 {
					t.Fatalf("expected optional fields to be empty, got rating=%q developers=%v", detail.Rating, detail.Details.Developers)
				}
			},
		},
		{
			name:     "game empty platform scores",
			category: domain.CategoryGame,
			href:     "https://www.metacritic.com/game/platformless",
			fixture:  "detail_game_empty_platform_scores.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if len(detail.Details.PlatformScores) != 0 {
					t.Fatalf("expected no platform scores, got %+v", detail.Details.PlatformScores)
				}
			},
		},
		{
			name:     "movie score order changed",
			category: domain.CategoryMovie,
			href:     "https://www.metacritic.com/movie/score-order",
			fixture:  "detail_movie_score_order_changed.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if detail.Metascore != "91" || detail.UserScore != "8.4" {
					t.Fatalf("expected both scores despite wrapper order change, got metascore=%q user=%q", detail.Metascore, detail.UserScore)
				}
			},
		},
		{
			name:     "movie label variation",
			category: domain.CategoryMovie,
			href:     "https://www.metacritic.com/movie/label-variation",
			fixture:  "detail_movie_label_variation.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if detail.ReleaseDate != "Aug 13, 2004" || detail.Rating != "PG" {
					t.Fatalf("label variations were not parsed: release_date=%q rating=%q", detail.ReleaseDate, detail.Rating)
				}
			},
		},
		{
			name:     "movie empty awards",
			category: domain.CategoryMovie,
			href:     "https://www.metacritic.com/movie/no-awards",
			fixture:  "detail_movie_empty_awards.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if len(detail.Details.Awards) != 0 {
					t.Fatalf("expected no awards, got %+v", detail.Details.Awards)
				}
			},
		},
		{
			name:     "movie missing global scores is still accepted",
			category: domain.CategoryMovie,
			href:     "https://www.metacritic.com/movie/missing-score",
			fixture:  "detail_movie_missing_score.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if detail.Title == "" {
					t.Fatal("expected title to still parse")
				}
				if detail.Metascore != "" || detail.UserScore != "" {
					t.Fatalf("expected missing scores to stay empty, got metascore=%q user=%q", detail.Metascore, detail.UserScore)
				}
			},
		},
		{
			name:     "tv empty seasons",
			category: domain.CategoryTV,
			href:     "https://www.metacritic.com/tv/no-seasons",
			fixture:  "detail_tv_empty_seasons.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if len(detail.Details.Seasons) != 0 {
					t.Fatalf("expected no seasons, got %+v", detail.Details.Seasons)
				}
			},
		},
		{
			name:     "tv summary read more trimmed",
			category: domain.CategoryTV,
			href:     "https://www.metacritic.com/tv/read-more",
			fixture:  "detail_tv_summary_read_more.html",
			assert: func(t *testing.T, detail domain.WorkDetail) {
				t.Helper()
				if strings.Contains(detail.Summary, "Read More") {
					t.Fatalf("expected read-more residue to be removed, got %q", detail.Summary)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			detail := parseDetailFixture(t, tt.category, tt.href, tt.fixture)
			tt.assert(t, detail)
		})
	}
}

func TestParseDetailRequiresStructuredErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		category    domain.Category
		href        string
		fixture     string
		wantSnippet string
	}{
		{
			name:        "missing title",
			category:    domain.CategoryMovie,
			href:        "https://www.metacritic.com/movie/missing-title",
			fixture:     "detail_missing_title.html",
			wantSnippet: "category=movie field=title stage=validate",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseDetail(tt.category, tt.href, strings.NewReader(readDetailFixture(t, tt.fixture)))
			if err == nil {
				t.Fatal("expected parse error")
			}
			if !strings.Contains(err.Error(), tt.wantSnippet) {
				t.Fatalf("expected error to contain %q, got %v", tt.wantSnippet, err)
			}
		})
	}
}

func parseDetailFixture(t *testing.T, category domain.Category, href string, fixture string) domain.WorkDetail {
	t.Helper()

	detail, err := ParseDetail(category, href, strings.NewReader(readDetailFixture(t, fixture)))
	if err != nil {
		t.Fatalf("ParseDetail(%s) error = %v", fixture, err)
	}
	return detail
}

func readDetailFixture(t *testing.T, name string) string {
	t.Helper()

	path := filepath.Join("testdata", name)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return string(body)
}
