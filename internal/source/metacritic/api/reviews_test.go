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

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestReviewPageAPIFetchContextFromFixture(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/composer/metacritic/pages/games-critic-reviews/baldurs-gate-3/web":
			_, _ = w.Write([]byte(readReviewFixture(t, "composer_game_critic.json")))
		case "/composer/metacritic/pages/shows-critic-reviews/example-show/web":
			_, _ = w.Write([]byte(readReviewFixture(t, "composer_tv_critic.json")))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	api := NewReviewPageAPI(server.URL, nil, 5*time.Second, 0)
	ctx := context.Background()

	gameCtx, err := api.FetchContext(ctx, domain.Work{
		Name:     "Fallback Game",
		Href:     "https://www.metacritic.com/game/baldurs-gate-3/",
		Category: domain.CategoryGame,
	}, domain.ReviewTypeCritic)
	if err != nil {
		t.Fatalf("FetchContext(game) error = %v", err)
	}
	if gameCtx.Title != "Baldur's Gate 3" || gameCtx.Slug != "baldurs-gate-3" || gameCtx.ReviewCount != 119 || len(gameCtx.Platforms) != 2 {
		t.Fatalf("unexpected game context: %+v", gameCtx)
	}

	tvCtx, err := api.FetchContext(ctx, domain.Work{
		Name:     "Example Show",
		Href:     "https://www.metacritic.com/tv/example-show/",
		Category: domain.CategoryTV,
	}, domain.ReviewTypeCritic)
	if err != nil {
		t.Fatalf("FetchContext(tv) error = %v", err)
	}
	if len(tvCtx.Seasons) != 2 || tvCtx.Seasons[0].Label != "Season 1" || tvCtx.Seasons[1].Label != "Season 2" {
		t.Fatalf("unexpected tv seasons: %+v", tvCtx.Seasons)
	}
}

func TestReviewListAPIFetchPageFromFixtures(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.RawQuery, "offset=0"):
			_, _ = w.Write([]byte(readReviewFixture(t, "list_critic.json")))
		case strings.Contains(r.URL.RawQuery, "offset=2"):
			_, _ = w.Write([]byte(readReviewFixture(t, "list_empty.json")))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	api := NewReviewListAPI(server.URL, nil, 5*time.Second, 0)
	page, err := api.FetchPage(
		context.Background(),
		domain.Work{Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame},
		domain.ReviewTypeCritic,
		domain.ReviewSentimentPositive,
		domain.ReviewSortScore,
		"",
		0,
		20,
	)
	if err != nil {
		t.Fatalf("FetchPage() error = %v", err)
	}
	if page.TotalResults != 2 || len(page.Items) != 2 {
		t.Fatalf("unexpected page: %+v", page)
	}
	if page.Items[0].PublicationName != "PC Gamer" || page.Items[0].AuthorName != "Alice" || page.Items[0].Username != "" {
		t.Fatalf("unexpected critic record normalization: %+v", page.Items[0])
	}
	if page.Items[1].PlatformKey != "playstation-5" {
		t.Fatalf("expected platform from reviewedProduct, got %+v", page.Items[1])
	}
	if !strings.Contains(page.RawPayload, "\"totalResults\":2") {
		t.Fatalf("expected normalized payload, got %q", page.RawPayload)
	}

	emptyPage, err := api.FetchPage(
		context.Background(),
		domain.Work{Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame},
		domain.ReviewTypeCritic,
		domain.ReviewSentimentAll,
		"",
		"",
		2,
		20,
	)
	if err != nil {
		t.Fatalf("FetchPage(empty) error = %v", err)
	}
	if emptyPage.TotalResults != 0 || len(emptyPage.Items) != 0 {
		t.Fatalf("unexpected empty page: %+v", emptyPage)
	}
}

func TestReviewListAPIFetchPageUserAndMissingFieldVariants(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/reviews/metacritic/user/games/baldurs-gate-3/web":
			_, _ = w.Write([]byte(readReviewFixture(t, "list_user.json")))
		case "/reviews/metacritic/user/games/sparse-game/web":
			_, _ = w.Write([]byte(readReviewFixture(t, "list_missing_fields.json")))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	api := NewReviewListAPI(server.URL, nil, 5*time.Second, 0)
	userPage, err := api.FetchPage(
		context.Background(),
		domain.Work{Href: "https://www.metacritic.com/game/baldurs-gate-3/", Category: domain.CategoryGame},
		domain.ReviewTypeUser,
		domain.ReviewSentimentAll,
		"",
		"pc",
		0,
		20,
	)
	if err != nil {
		t.Fatalf("FetchPage(user) error = %v", err)
	}
	if len(userPage.Items) != 1 {
		t.Fatalf("unexpected user page items: %+v", userPage.Items)
	}
	record := userPage.Items[0]
	if record.Username != "player1" || record.UserSlug != "player1" || record.AuthorName != "" || record.PublicationName != "" {
		t.Fatalf("unexpected user record normalization: %+v", record)
	}
	if record.PlatformKey != "pc" || reviewInt64Value(record.ThumbsUp) != 12 || reviewBoolValue(record.SpoilerFlag) {
		t.Fatalf("unexpected user record fields: %+v", record)
	}

	sparsePage, err := api.FetchPage(
		context.Background(),
		domain.Work{Href: "https://www.metacritic.com/game/sparse-game/", Category: domain.CategoryGame},
		domain.ReviewTypeUser,
		domain.ReviewSentimentAll,
		"",
		"",
		0,
		20,
	)
	if err != nil {
		t.Fatalf("FetchPage(sparse) error = %v", err)
	}
	if len(sparsePage.Items) != 1 {
		t.Fatalf("unexpected sparse items: %+v", sparsePage.Items)
	}
	if sparsePage.Items[0].PlatformKey != "switch" || sparsePage.Items[0].ReviewURL != "" || !reviewBoolValue(sparsePage.Items[0].SpoilerFlag) {
		t.Fatalf("unexpected sparse normalization: %+v", sparsePage.Items[0])
	}
}

func TestReviewListAPIFetchPageRejectsInvalidPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fixture  string
		wantText string
	}{
		{name: "missing data", fixture: "list_missing_data.json", wantText: "missing data"},
		{name: "invalid json", fixture: "list_invalid.json", wantText: "decode review list response"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(readReviewFixture(t, tt.fixture)))
			}))
			defer server.Close()

			api := NewReviewListAPI(server.URL, nil, 5*time.Second, 0)
			_, err := api.FetchPage(
				context.Background(),
				domain.Work{Href: "https://www.metacritic.com/movie/pk/", Category: domain.CategoryMovie},
				domain.ReviewTypeCritic,
				domain.ReviewSentimentAll,
				"",
				"",
				0,
				20,
			)
			if err == nil || !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("expected error containing %q, got %v", tt.wantText, err)
			}
		})
	}
}

func TestBuildReviewRecordNormalizationRules(t *testing.T) {
	t.Parallel()

	work := domain.Work{
		Href:     "https://www.metacritic.com/game/baldurs-gate-3/",
		Category: domain.CategoryGame,
	}
	scoreValue := 8.5

	tests := []struct {
		name       string
		reviewType domain.ReviewType
		requested  string
		item       map[string]any
		assert     func(t *testing.T, record domain.ReviewRecord)
	}{
		{
			name:       "critic maps author and publication only",
			reviewType: domain.ReviewTypeCritic,
			item: map[string]any{
				"id":              "critic-1",
				"url":             "/game/foo/critic-1",
				"date":            "2023-08-03",
				"score":           100.0,
				"quote":           "A masterpiece.",
				"author":          "Alice",
				"authorSlug":      "alice",
				"publicationName": "PC Gamer",
				"publicationSlug": "pc-gamer",
				"platform": map[string]any{
					"slug": "pc",
				},
			},
			assert: func(t *testing.T, record domain.ReviewRecord) {
				if record.AuthorName != "Alice" || record.AuthorSlug != "alice" || record.Username != "" || record.UserSlug != "" {
					t.Fatalf("unexpected critic author mapping: %+v", record)
				}
				if record.PublicationName != "PC Gamer" || record.PlatformKey != "pc" {
					t.Fatalf("unexpected critic record: %+v", record)
				}
			},
		},
		{
			name:       "user maps author into username only",
			reviewType: domain.ReviewTypeUser,
			requested:  "xbox-series-x",
			item: map[string]any{
				"id":         "user-1",
				"author":     "player1",
				"authorSlug": "player1",
				"reviewDate": "2023-08-05",
				"score":      scoreValue,
				"quote":      "Loved it",
				"thumbsUp":   "12",
				"thumbsDown": "2",
				"spoiler":    "1",
				"season":     "Season 1",
				"platform": map[string]any{
					"slug": "pc",
				},
			},
			assert: func(t *testing.T, record domain.ReviewRecord) {
				if record.Username != "player1" || record.UserSlug != "player1" || record.AuthorName != "" || record.AuthorSlug != "" {
					t.Fatalf("unexpected user mapping: %+v", record)
				}
				if record.PlatformKey != "xbox-series-x" || reviewInt64Value(record.ThumbsUp) != 12 || !reviewBoolValue(record.SpoilerFlag) || record.SeasonLabel != "Season 1" {
					t.Fatalf("unexpected user fields: %+v", record)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			record := buildReviewRecord(work, tt.reviewType, tt.requested, tt.item)
			if record.ReviewKey == "" || record.SourcePayloadJSON == "" {
				t.Fatalf("expected key and payload, got %+v", record)
			}
			tt.assert(t, record)
		})
	}
}

func TestBuildReviewRecordKeepsReviewKeyStableAcrossIrrelevantFieldChanges(t *testing.T) {
	t.Parallel()

	work := domain.Work{
		Href:     "https://www.metacritic.com/game/baldurs-gate-3/",
		Category: domain.CategoryGame,
	}

	userBase := map[string]any{
		"id":         "",
		"author":     "player1",
		"authorSlug": "player1",
		"reviewDate": "2023-08-05",
		"score":      8.5,
		"quote":      "Loved it",
	}
	userVariant := map[string]any{
		"id":              "",
		"author":          "player1",
		"authorSlug":      "player1",
		"reviewDate":      "2023-08-05",
		"score":           8.5,
		"quote":           "Loved it",
		"publicationName": "Ignored Publication",
	}
	baseKey := buildReviewRecord(work, domain.ReviewTypeUser, "", userBase).ReviewKey
	variantKey := buildReviewRecord(work, domain.ReviewTypeUser, "", userVariant).ReviewKey
	if baseKey != variantKey {
		t.Fatalf("expected user review key stability, got %q vs %q", baseKey, variantKey)
	}

	criticBase := map[string]any{
		"author":          "Alice",
		"authorSlug":      "alice",
		"publicationSlug": "pc-gamer",
		"reviewDate":      "2023-08-03",
		"quote":           "A masterpiece.",
	}
	criticVariant := map[string]any{
		"author":          "Alice",
		"authorSlug":      "alice",
		"publicationSlug": "pc-gamer",
		"reviewDate":      "2023-08-03",
		"quote":           "A masterpiece.",
		"thumbsUp":        12,
	}
	baseCriticKey := buildReviewRecord(work, domain.ReviewTypeCritic, "", criticBase).ReviewKey
	variantCriticKey := buildReviewRecord(work, domain.ReviewTypeCritic, "", criticVariant).ReviewKey
	if baseCriticKey != variantCriticKey {
		t.Fatalf("expected critic review key stability, got %q vs %q", baseCriticKey, variantCriticKey)
	}
}

func TestFetchJSONWithRetriesHTTPBehavior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statuses     []int
		maxRetries   int
		wantRequests int
		wantErr      bool
	}{
		{name: "429 retries once", statuses: []int{http.StatusTooManyRequests, http.StatusOK}, maxRetries: 1, wantRequests: 2},
		{name: "500 retries once", statuses: []int{http.StatusInternalServerError, http.StatusOK}, maxRetries: 1, wantRequests: 2},
		{name: "403 does not retry", statuses: []int{http.StatusForbidden}, maxRetries: 2, wantRequests: 1, wantErr: true},
		{name: "404 does not retry", statuses: []int{http.StatusNotFound}, maxRetries: 2, wantRequests: 1, wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				status := tt.statuses[requests]
				requests++
				w.WriteHeader(status)
				if status == http.StatusOK {
					_, _ = w.Write([]byte(`{"ok":true}`))
					return
				}
				_, _ = w.Write([]byte(`{"error":"boom"}`))
			}))
			defer server.Close()

			body, err := fetchJSONWithRetries(context.Background(), server.Client(), server.URL, tt.maxRetries)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if string(body) != `{"ok":true}` {
					t.Fatalf("unexpected body: %s", string(body))
				}
			}
			if requests != tt.wantRequests {
				t.Fatalf("expected %d requests, got %d", tt.wantRequests, requests)
			}
		})
	}
}

func TestBuildReviewListURLIncludesFilters(t *testing.T) {
	t.Parallel()

	reqURL, err := buildReviewListURL(
		"https://backend.metacritic.com",
		domain.CategoryGame,
		domain.ReviewTypeCritic,
		domain.ReviewSentimentNegative,
		domain.ReviewSortPublication,
		"https://www.metacritic.com/game/baldurs-gate-3/",
		"pc",
		20,
		10,
	)
	if err != nil {
		t.Fatalf("buildReviewListURL() error = %v", err)
	}
	if !strings.Contains(reqURL, "filterBySentiment=negative") || !strings.Contains(reqURL, "sort=publication") || !strings.Contains(reqURL, "platform=pc") {
		t.Fatalf("expected URL to include filters, got %s", reqURL)
	}
}

func readReviewFixture(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("testdata", "reviews", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return string(content)
}

func reviewInt64Value(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func reviewBoolValue(v *bool) bool {
	return v != nil && *v
}
