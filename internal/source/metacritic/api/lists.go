package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/GoFurry/metacritic-harvester/internal/domain"
)

const finderPageSize = 24

func BuildFinderListURLForTest(baseURL string, task domain.ListTask, page int) (string, error) {
	return buildFinderListURL(baseURL, task, page)
}

func FinderPageSizeForTest() int {
	return finderPageSize
}

type FinderAPI struct {
	baseURL    string
	client     *http.Client
	maxRetries int
}

type FinderListPage struct {
	TotalResults int
	CurrentPage  int
	LastPage     int
	Items        []FinderListItem
}

type FinderListItem struct {
	Work      domain.Work
	Metascore string
	UserScore string
}

func NewFinderAPI(baseURL string, transport *http.Transport, timeout time.Duration, maxRetries int) *FinderAPI {
	var roundTripper http.RoundTripper
	if transport != nil {
		roundTripper = transport
	}
	return &FinderAPI{
		baseURL:    strings.TrimRight(baseURL, "/"),
		maxRetries: maxRetries,
		client: &http.Client{
			Timeout:   timeout,
			Transport: roundTripper,
		},
	}
}

func (a *FinderAPI) FetchPage(ctx context.Context, task domain.ListTask, page int) (FinderListPage, error) {
	reqURL, err := buildFinderListURL(a.baseURL, task, page)
	if err != nil {
		return FinderListPage{}, err
	}

	body, err := fetchJSONWithRetries(ctx, a.client, reqURL, a.maxRetries)
	if err != nil {
		return FinderListPage{}, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return FinderListPage{}, fmt.Errorf("decode finder response: %w", err)
	}

	dataMap, ok := asMap(raw["data"])
	if !ok {
		return FinderListPage{}, fmt.Errorf("finder response missing data")
	}

	items := asSlice(dataMap["items"])
	result := FinderListPage{
		TotalResults: intFromAny(dataMap["totalResults"]),
		CurrentPage:  page,
		LastPage:     page,
		Items:        make([]FinderListItem, 0, len(items)),
	}
	if lastPage := parseFinderLastPage(raw); lastPage > 0 {
		result.LastPage = lastPage
	} else if result.TotalResults > 0 {
		result.LastPage = (result.TotalResults + finderPageSize - 1) / finderPageSize
	}

	for _, item := range items {
		itemMap, ok := asMap(item)
		if !ok {
			continue
		}
		work, metascore, userScore, ok := mapFinderListItem(task.Category, itemMap)
		if !ok {
			continue
		}
		result.Items = append(result.Items, FinderListItem{
			Work:      work,
			Metascore: metascore,
			UserScore: userScore,
		})
	}

	return result, nil
}

func buildFinderListURL(baseURL string, task domain.ListTask, page int) (string, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "finder", "metacritic", "web")

	q := u.Query()
	q.Set("mcoTypeId", strconv.Itoa(finderMCOTypeID(task.Category)))
	q.Set("limit", strconv.Itoa(finderPageSize))
	if page <= 1 {
		q.Set("offset", "0")
	} else {
		q.Set("offset", strconv.Itoa((page-1)*finderPageSize))
	}
	q.Set("sortBy", finderSortBy(task.Metric))

	if task.Filter.ReleaseYearMin != nil {
		q.Set("releaseYearMin", strconv.Itoa(*task.Filter.ReleaseYearMin))
	}
	if task.Filter.ReleaseYearMax != nil {
		q.Set("releaseYearMax", strconv.Itoa(*task.Filter.ReleaseYearMax))
	}
	if len(task.Filter.ReleaseTypes) > 0 {
		q.Set("releaseType", strings.Join(task.Filter.ReleaseTypes, ","))
	}
	if len(task.Filter.Genres) > 0 {
		q.Set("genres", strings.Join(mapFinderGenres(task.Filter.Genres), ","))
	}

	if len(task.Filter.Platforms) > 0 {
		platformIDs, err := mapFinderPlatformIDs(task.Filter.Platforms)
		if err != nil {
			return "", err
		}
		q.Set("gamePlatformIds", strings.Join(platformIDs, ","))
	}
	if len(task.Filter.Networks) > 0 {
		networkIDs, err := mapFinderNetworkIDs(task.Filter.Networks)
		if err != nil {
			return "", err
		}
		q.Set("streamingNetworkIds", strings.Join(networkIDs, ","))
	}

	u.RawQuery = q.Encode()
	return u.String(), nil
}

func finderMCOTypeID(category domain.Category) int {
	switch category {
	case domain.CategoryGame:
		return 13
	case domain.CategoryMovie:
		return 2
	case domain.CategoryTV:
		return 1
	default:
		return 0
	}
}

func finderSortBy(metric domain.Metric) string {
	switch metric {
	case domain.MetricUserScore:
		return "-userScore"
	case domain.MetricNewest:
		return "-releaseDate"
	default:
		return "-metaScore"
	}
}

func mapFinderListItem(category domain.Category, itemMap map[string]any) (domain.Work, string, string, bool) {
	slug := scalarString(itemMap["slug"])
	if slug == "" {
		return domain.Work{}, "", "", false
	}

	work := domain.Work{
		Name:        scalarString(itemMap["title"]),
		Href:        finderWorkHref(category, slug),
		ImageURL:    finderImageURL(itemMap),
		ReleaseDate: humanizeAPIDate(scalarString(itemMap["releaseDate"])),
		Category:    category,
	}
	if strings.TrimSpace(work.Name) == "" || strings.TrimSpace(work.Href) == "" {
		return domain.Work{}, "", "", false
	}

	metascore := ""
	if scoreSummary, ok := asMap(itemMap["criticScoreSummary"]); ok {
		metascore = scalarString(scoreSummary["score"])
	}
	userScore := ""
	if userScoreMap, ok := asMap(itemMap["userScore"]); ok {
		userScore = scalarString(userScoreMap["score"])
	}
	return work, metascore, userScore, true
}

func parseFinderLastPage(raw map[string]any) int {
	links, ok := asMap(raw["links"])
	if !ok {
		return 0
	}
	if last, ok := asMap(links["last"]); ok {
		if meta, ok := asMap(last["meta"]); ok {
			pageNum := intFromAny(meta["pageNum"])
			if pageNum > 0 {
				return pageNum
			}
		}
	}
	if self, ok := asMap(links["self"]); ok {
		if meta, ok := asMap(self["meta"]); ok {
			pageNum := intFromAny(meta["pageNum"])
			if pageNum > 0 {
				return pageNum
			}
		}
	}
	return 0
}

func finderWorkHref(category domain.Category, slug string) string {
	return fmt.Sprintf("https://www.metacritic.com/%s/%s/", category, strings.Trim(strings.TrimSpace(slug), "/"))
}

func finderImageURL(itemMap map[string]any) string {
	image, ok := asMap(itemMap["image"])
	if !ok {
		return ""
	}
	if direct := firstNonEmpty(scalarString(image["path"]), scalarString(image["imageUrl"])); direct != "" {
		return direct
	}
	bucketPath := strings.TrimSpace(scalarString(image["bucketPath"]))
	if bucketPath == "" {
		return ""
	}
	return "https://www.metacritic.com/a/img/catalog" + bucketPath
}

func humanizeAPIDate(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return raw
	}
	return parsed.Format("Jan 2, 2006")
}

func mapFinderGenres(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		value = strings.ReplaceAll(value, "-", " ")
		parts := strings.Fields(value)
		for i, part := range parts {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
		result = append(result, strings.Join(parts, " "))
	}
	return result
}

func mapFinderPlatformIDs(values []string) ([]string, error) {
	result := make([]string, 0, len(values))
	for _, value := range values {
		mapped, ok := knownFinderPlatformIDs[strings.ToLower(strings.TrimSpace(value))]
		if ok {
			result = append(result, mapped)
			continue
		}
		if _, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			result = append(result, strings.TrimSpace(value))
			continue
		}
		return nil, fmt.Errorf("finder api does not support unresolved platform %q", value)
	}
	return result, nil
}

func mapFinderNetworkIDs(values []string) ([]string, error) {
	result := make([]string, 0, len(values))
	for _, value := range values {
		mapped, ok := knownFinderNetworkIDs[strings.ToLower(strings.TrimSpace(value))]
		if ok {
			result = append(result, mapped)
			continue
		}
		if _, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			result = append(result, strings.TrimSpace(value))
			continue
		}
		return nil, fmt.Errorf("finder api does not support unresolved network %q", value)
	}
	return result, nil
}

var knownFinderPlatformIDs = map[string]string{
	"pc":              "1500000019",
	"playstation-4":   "1500000120",
	"ps4":             "1500000120",
	"xbox-one":        "1500000121",
	"switch":          "1500000122",
	"playstation-5":   "1500000128",
	"ps5":             "1500000128",
	"xbox-series-x":   "1500000129",
	"xbox-series-xs":  "1500000129",
	"xbox-series-x|s": "1500000129",
}

var knownFinderNetworkIDs = map[string]string{
	"netflix": "1943",
}
