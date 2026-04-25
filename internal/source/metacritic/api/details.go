package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/GoFurry/metacritic-harvester/internal/domain"
)

type ComposerAPI struct {
	baseURL    string
	client     *http.Client
	maxRetries int
}

func NewComposerAPI(baseURL string, transport *http.Transport, timeout time.Duration, maxRetries int) *ComposerAPI {
	var roundTripper http.RoundTripper
	if transport != nil {
		roundTripper = transport
	}
	return &ComposerAPI{
		baseURL:    strings.TrimRight(baseURL, "/"),
		maxRetries: maxRetries,
		client: &http.Client{
			Timeout:   timeout,
			Transport: roundTripper,
		},
	}
}

func (a *ComposerAPI) Fetch(ctx context.Context, work domain.Work) (domain.WorkDetail, error) {
	reqURL, err := buildComposerDetailURL(a.baseURL, work)
	if err != nil {
		return domain.WorkDetail{}, err
	}

	body, err := fetchJSONWithRetries(ctx, a.client, reqURL, a.maxRetries)
	if err != nil {
		return domain.WorkDetail{}, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return domain.WorkDetail{}, fmt.Errorf("decode composer response: %w", err)
	}

	components := asSlice(raw["components"])
	product := composerProductItem(components)
	if product == nil {
		return domain.WorkDetail{}, fmt.Errorf("composer response missing product component")
	}

	detail := domain.WorkDetail{
		WorkHref:      work.Href,
		Category:      work.Category,
		Title:         scalarString(product["title"]),
		Summary:       scalarString(product["description"]),
		ReleaseDate:   humanizeAPIDate(firstNonEmpty(scalarString(product["releaseDateText"]), scalarString(product["releaseDate"]))),
		Rating:        scalarString(product["rating"]),
		Duration:      scalarString(product["duration"]),
		Tagline:       scalarString(product["tagline"]),
		LastFetchedAt: time.Now().UTC(),
	}

	if scoreSummary, ok := asMap(product["criticScoreSummary"]); ok {
		detail.Metascore = scalarString(scoreSummary["score"])
		detail.MetascoreSentiment = scalarString(scoreSummary["sentiment"])
		detail.MetascoreReviewCount = intFromAny(scoreSummary["reviewCount"])
	}
	if userScore, ok := asMap(product["userScore"]); ok {
		detail.UserScore = scalarString(userScore["score"])
		detail.UserScoreSentiment = scalarString(userScore["sentiment"])
		detail.UserScoreCount = intFromAny(userScore["reviewCount"])
	}

	detail.Details.Genres = composerGenres(product["genres"])

	switch work.Category {
	case domain.CategoryGame:
		detail.Details.CurrentPlatform = scalarString(product["platform"])
		detail.Details.Platforms, detail.Details.PlatformScores = composerPlatforms(product["platforms"])
		detail.Details.Developers = composerCompaniesByType(product["production"], "developer")
		detail.Details.Publishers = composerCompaniesByType(product["production"], "publisher")
	case domain.CategoryMovie:
		detail.Details.Directors = composerCrewByRole(product["production"], "directed by")
		detail.Details.Writers = composerCrewByRole(product["production"], "written by")
		detail.Details.ProductionCompanies = composerProductionCompanies(product["production"])
	case domain.CategoryTV:
		detail.Details.ProductionCompanies = composerProductionCompanies(product["production"])
		numberOfSeasons := intFromAny(product["numberOfSeasons"])
		if numberOfSeasons > 0 {
			detail.Details.NumberOfSeasons = fmt.Sprintf("%d", numberOfSeasons)
		}
	}

	return detail, nil
}

func buildComposerDetailURL(baseURL string, work domain.Work) (string, error) {
	slug := slugFromWorkHref(work.Category, work.Href)
	if slug == "" {
		return "", fmt.Errorf("composer detail slug is empty")
	}

	section, err := reviewSectionPath(work.Category)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "composer", "metacritic", "pages", section, slug, "web")
	q := u.Query()
	q.Set("contentOnly", "true")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func composerProductItem(components []any) map[string]any {
	for _, component := range components {
		componentMap, ok := asMap(component)
		if !ok {
			continue
		}
		meta, _ := asMap(componentMap["meta"])
		if scalarString(meta["componentName"]) != "product" {
			continue
		}
		dataMap, _ := asMap(componentMap["data"])
		itemMap, _ := asMap(dataMap["item"])
		if itemMap != nil {
			return itemMap
		}
	}
	return nil
}

func composerGenres(value any) []string {
	items := asSlice(value)
	result := make([]string, 0, len(items))
	for _, item := range items {
		itemMap, ok := asMap(item)
		if !ok {
			continue
		}
		name := scalarString(itemMap["name"])
		if strings.TrimSpace(name) != "" {
			result = append(result, name)
		}
	}
	return result
}

func composerPlatforms(value any) ([]string, []domain.PlatformScore) {
	items := asSlice(value)
	names := make([]string, 0, len(items))
	scores := make([]domain.PlatformScore, 0, len(items))
	for _, item := range items {
		itemMap, ok := asMap(item)
		if !ok {
			continue
		}
		name := scalarString(itemMap["name"])
		if name != "" {
			names = append(names, name)
		}
		platformScore := domain.PlatformScore{
			Platform: name,
		}
		if scoreSummary, ok := asMap(itemMap["criticScoreSummary"]); ok {
			platformScore.Href = normalizeURL(scalarString(scoreSummary["url"]))
			platformScore.Metascore = scalarString(scoreSummary["score"])
			platformScore.CriticReviewCount = intFromAny(scoreSummary["reviewCount"])
		}
		if platformScore.Platform != "" || platformScore.Href != "" || platformScore.Metascore != "" {
			scores = append(scores, platformScore)
		}
	}
	return names, scores
}

func composerCompaniesByType(productionValue any, typeName string) []string {
	production, ok := asMap(productionValue)
	if !ok {
		return nil
	}
	companies := asSlice(production["companies"])
	result := make([]string, 0, len(companies))
	for _, item := range companies {
		itemMap, ok := asMap(item)
		if !ok {
			continue
		}
		if strings.EqualFold(scalarString(itemMap["typeName"]), typeName) {
			name := scalarString(itemMap["name"])
			if name != "" {
				result = append(result, name)
			}
		}
	}
	return result
}

func composerProductionCompanies(productionValue any) []string {
	production, ok := asMap(productionValue)
	if !ok {
		return nil
	}
	companies := asSlice(production["companies"])
	result := make([]string, 0, len(companies))
	for _, item := range companies {
		itemMap, ok := asMap(item)
		if !ok {
			continue
		}
		name := scalarString(itemMap["name"])
		if name == "" {
			continue
		}
		if typeName := strings.TrimSpace(strings.ToLower(scalarString(itemMap["typeName"]))); typeName == "developer" || typeName == "publisher" {
			continue
		}
		result = append(result, name)
	}
	return result
}

func composerCrewByRole(productionValue any, role string) []string {
	production, ok := asMap(productionValue)
	if !ok {
		return nil
	}
	crewItems := asSlice(production["crew"])
	result := make([]string, 0, len(crewItems))
	for _, item := range crewItems {
		itemMap, ok := asMap(item)
		if !ok {
			continue
		}
		if !strings.EqualFold(scalarString(itemMap["roleTypeGroupName"]), role) {
			continue
		}
		name := firstNonEmpty(scalarString(itemMap["fullName"]), scalarString(itemMap["name"]))
		if name != "" {
			result = append(result, name)
		}
	}
	return result
}
