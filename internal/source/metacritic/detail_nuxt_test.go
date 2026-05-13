package metacritic

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func TestExtractNuxtDataFindsInlineScript(t *testing.T) {
	t.Parallel()

	doc := mustNewDocument(t, `<html><body><script id="__NUXT_DATA__">["ok"]</script></body></html>`)

	raw, found, err := ExtractNuxtData(doc)
	if err != nil {
		t.Fatalf("ExtractNuxtData() error = %v", err)
	}
	if !found || raw != `["ok"]` {
		t.Fatalf("unexpected result: found=%v raw=%q", found, raw)
	}
}

func TestResolveNuxtValueRecursesAndStopsOnCycles(t *testing.T) {
	t.Parallel()

	root := []any{
		map[string]any{"title": float64(1)},
		"hello",
		float64(3),
		float64(2),
	}

	resolved := ResolveNuxtValue(root, float64(0))
	object, ok := resolved.(map[string]any)
	if !ok {
		t.Fatalf("ResolveNuxtValue() type = %T", resolved)
	}
	if object["title"] != "hello" {
		t.Fatalf("resolved title = %#v", object["title"])
	}

	cyclic := ResolveNuxtValue(root, float64(2))
	if _, ok := cyclic.(float64); !ok {
		t.Fatalf("expected cyclic resolution to stop at numeric sentinel, got %T", cyclic)
	}
	if invalid := ResolveNuxtValue(root, float64(999)); invalid != float64(999) {
		t.Fatalf("expected invalid index to stay unchanged, got %#v", invalid)
	}
}

func TestParseWhereToBuyFromSample(t *testing.T) {
	t.Parallel()

	root := parseNuxtSampleRoot(t, "metacritic-nuxt-data-game-sample.txt")
	options, found, err := ParseWhereToBuy(root)
	if err != nil {
		t.Fatalf("ParseWhereToBuy() error = %v", err)
	}
	if !found || len(options) == 0 {
		t.Fatalf("expected where-to-buy options, got found=%v len=%d", found, len(options))
	}

	amazon := findBuyOption(options, "PlayStation 4", "Amazon")
	if amazon == nil {
		t.Fatalf("expected Amazon PlayStation 4 option, got %+v", options)
	}
	if amazon.Price == nil || *amazon.Price != 19.99 || amazon.PurchaseType != "Buy" || amazon.LinkURL == "" {
		t.Fatalf("unexpected Amazon option: %+v", *amazon)
	}

	bestBuy := findBuyOption(options, "Xbox One", "Best Buy")
	if bestBuy == nil {
		t.Fatalf("expected Xbox One Best Buy option, got %+v", options)
	}
	if bestBuy.OriginalPrice == nil || *bestBuy.OriginalPrice != 36.99 || bestBuy.DiscountPercentage == nil || *bestBuy.DiscountPercentage != 18.92 {
		t.Fatalf("unexpected Best Buy discount fields: %+v", *bestBuy)
	}
}

func TestParseWhereToWatchMovieSample(t *testing.T) {
	t.Parallel()

	root := parseNuxtSampleRoot(t, "metacritic-nuxt-data-movie-sample.txt")
	groups, found, err := ParseWhereToWatch(root)
	if err != nil {
		t.Fatalf("ParseWhereToWatch() error = %v", err)
	}
	if !found || len(groups) == 0 {
		t.Fatalf("expected where-to-watch groups, got found=%v len=%d", found, len(groups))
	}

	amazonRent := findWatchGroup(groups, "rent", "Amazon Video")
	if amazonRent == nil {
		t.Fatalf("expected Amazon Video rent group, got %+v", groups)
	}
	if amazonRent.Monetization != "rent" || amazonRent.LinkURL == "" {
		t.Fatalf("unexpected Amazon Video group: %+v", *amazonRent)
	}
	if len(amazonRent.Options) == 0 || amazonRent.Options[0].OfferType != "DEFAULT" || amazonRent.Options[0].QualityType != "4K" {
		t.Fatalf("unexpected Amazon Video options: %+v", amazonRent.Options)
	}
	if amazonRent.Options[0].OptionPrice == nil || *amazonRent.Options[0].OptionPrice != 3.99 {
		t.Fatalf("unexpected Amazon Video price: %+v", amazonRent.Options[0])
	}
}

func TestParseWhereToWatchTVSample(t *testing.T) {
	t.Parallel()

	root := parseNuxtSampleRoot(t, "metacritic-nuxt-data-tv-sample.txt")
	groups, found, err := ParseWhereToWatch(root)
	if err != nil {
		t.Fatalf("ParseWhereToWatch() error = %v", err)
	}
	if !found || len(groups) == 0 {
		t.Fatalf("expected where-to-watch groups, got found=%v len=%d", found, len(groups))
	}

	hboMax := findWatchGroup(groups, "flatrate", "HBO Max")
	if hboMax == nil {
		t.Fatalf("expected HBO Max flatrate group, got %+v", groups)
	}
	if hboMax.NumberOfSeasons != 6 || hboMax.ProviderIcon == "" || hboMax.LinkURL == "" {
		t.Fatalf("unexpected HBO Max group: %+v", *hboMax)
	}
	if len(hboMax.Options) == 0 || hboMax.Options[0].QualityType != "HD" || hboMax.Options[0].OfferType != "SEASON" {
		t.Fatalf("unexpected HBO Max option payload: %+v", hboMax.Options)
	}
}

func TestParseDetailDocumentAddsNuxtDetailsWithoutBreakingHTMLParsing(t *testing.T) {
	t.Parallel()

	gameRaw := readNuxtSample(t, "metacritic-nuxt-data-game-sample.txt")
	gameDoc := mustNewDocument(t, minimalDetailHTML("Nuxt Game", gameRaw))
	gameDetail, err := ParseDetailDocument(domain.CategoryGame, "https://www.metacritic.com/game/nuxt-game", gameDoc)
	if err != nil {
		t.Fatalf("ParseDetailDocument(game) error = %v", err)
	}
	if gameDetail.Title != "Nuxt Game" || len(gameDetail.Details.WhereToBuy) == 0 {
		t.Fatalf("expected where_to_buy to be populated, got %+v", gameDetail)
	}

	tvRaw := readNuxtSample(t, "metacritic-nuxt-data-tv-sample.txt")
	tvDoc := mustNewDocument(t, minimalDetailHTML("Nuxt Show", tvRaw))
	tvDetail, err := ParseDetailDocument(domain.CategoryTV, "https://www.metacritic.com/tv/nuxt-show", tvDoc)
	if err != nil {
		t.Fatalf("ParseDetailDocument(tv) error = %v", err)
	}
	if tvDetail.Title != "Nuxt Show" || len(tvDetail.Details.WhereToWatch) == 0 {
		t.Fatalf("expected where_to_watch to be populated, got %+v", tvDetail)
	}
}

func TestParseDetailFailsWhenNuxtScriptIsInvalid(t *testing.T) {
	t.Parallel()

	doc := mustNewDocument(t, minimalDetailHTML("Broken Nuxt", `{not-json}`))
	_, err := ParseDetailDocument(domain.CategoryGame, "https://www.metacritic.com/game/broken-nuxt", doc)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "field=nuxt_data stage=parse") {
		t.Fatalf("expected structured nuxt parse error, got %v", err)
	}
}

func parseNuxtSampleRoot(t *testing.T, name string) []any {
	t.Helper()

	root, err := parseNuxtRoot(readNuxtSample(t, name))
	if err != nil {
		t.Fatalf("parseNuxtRoot(%s) error = %v", name, err)
	}
	return root
}

func readNuxtSample(t *testing.T, name string) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}

	path := filepath.Join(filepath.Dir(file), "testdata", name)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	raw := strings.TrimSpace(string(body))
	if idx := strings.Index(raw, "</script>"); idx >= 0 {
		raw = raw[:idx]
	}
	if start := strings.Index(raw, ">"); strings.HasPrefix(raw, "<script") && start >= 0 {
		raw = raw[start+1:]
	}
	return strings.TrimSpace(raw)
}

func minimalDetailHTML(title string, nuxtRaw string) string {
	return `<html><body><h1 class="hero-title__text">` + title + `</h1><script id="__NUXT_DATA__">` + nuxtRaw + `</script></body></html>`
}

func mustNewDocument(t *testing.T, html string) *goquery.Document {
	t.Helper()

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		t.Fatalf("NewDocumentFromReader() error = %v", err)
	}
	return doc
}

func findBuyOption(options []domain.BuyOption, groupName string, store string) *domain.BuyOption {
	for i := range options {
		if options[i].GroupName == groupName && options[i].Store == store {
			return &options[i]
		}
	}
	return nil
}

func findWatchGroup(groups []domain.WatchGroup, groupName string, provider string) *domain.WatchGroup {
	for i := range groups {
		if groups[i].GroupName == groupName && groups[i].ProviderName == provider {
			return &groups[i]
		}
	}
	return nil
}
