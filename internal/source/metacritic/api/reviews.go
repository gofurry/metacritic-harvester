package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

type ReviewPageAPI struct {
	baseURL    string
	client     *http.Client
	maxRetries int
}

type ReviewListAPI struct {
	baseURL    string
	client     *http.Client
	maxRetries int
}

type ReviewPageContext struct {
	WorkHref    string
	Category    domain.Category
	ReviewType  domain.ReviewType
	Slug        string
	Title       string
	Section     string
	Platforms   []ReviewPlatformContext
	Seasons     []ReviewSeasonContext
	ReviewCount int
}

type ReviewPlatformContext struct {
	Key         string
	Name        string
	ReviewCount int
}

type ReviewSeasonContext struct {
	Label string
	Href  string
}

func NewReviewPageAPI(baseURL string, transport http.RoundTripper, timeout time.Duration, maxRetries int) *ReviewPageAPI {
	var roundTripper http.RoundTripper
	if transport != nil {
		roundTripper = transport
	}
	return &ReviewPageAPI{
		baseURL:    strings.TrimRight(baseURL, "/"),
		maxRetries: maxRetries,
		client: &http.Client{
			Timeout:   timeout,
			Transport: roundTripper,
		},
	}
}

func NewReviewListAPI(baseURL string, transport http.RoundTripper, timeout time.Duration, maxRetries int) *ReviewListAPI {
	var roundTripper http.RoundTripper
	if transport != nil {
		roundTripper = transport
	}
	return &ReviewListAPI{
		baseURL:    strings.TrimRight(baseURL, "/"),
		maxRetries: maxRetries,
		client: &http.Client{
			Timeout:   timeout,
			Transport: roundTripper,
		},
	}
}

func (a *ReviewPageAPI) FetchContext(ctx context.Context, work domain.Work, reviewType domain.ReviewType) (ReviewPageContext, error) {
	reqURL, err := buildReviewPageURL(a.baseURL, work.Category, reviewType, work.Href)
	if err != nil {
		return ReviewPageContext{}, err
	}

	body, err := a.fetch(ctx, reqURL)
	if err != nil {
		return ReviewPageContext{}, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return ReviewPageContext{}, fmt.Errorf("decode review page response: %w", err)
	}

	ctxData := ReviewPageContext{
		WorkHref:   work.Href,
		Category:   work.Category,
		ReviewType: reviewType,
		Slug:       slugFromWorkHref(work.Category, work.Href),
	}

	if meta, ok := asMap(raw["meta"]); ok {
		ctxData.Section = scalarString(meta["section"])
	}

	components := asSlice(raw["components"])
	for _, component := range components {
		componentMap, ok := asMap(component)
		if !ok {
			continue
		}
		meta, _ := asMap(componentMap["meta"])
		componentName := scalarString(meta["componentName"])
		componentType := scalarString(meta["componentType"])
		dataMap, _ := asMap(componentMap["data"])

		switch componentName {
		case "product":
			itemMap, _ := asMap(dataMap["item"])
			if itemMap == nil {
				continue
			}
			ctxData.Title = firstNonEmpty(scalarString(itemMap["title"]), ctxData.Title)
			ctxData.Slug = firstNonEmpty(scalarString(itemMap["slug"]), ctxData.Slug)
			if platforms := asSlice(itemMap["platforms"]); len(platforms) > 0 {
				ctxData.Platforms = append(ctxData.Platforms, extractPlatforms(platforms)...)
			}
		case "critic-score-summary", "user-score-summary":
			itemMap, _ := asMap(dataMap["item"])
			if itemMap == nil {
				continue
			}
			ctxData.ReviewCount = maxInt(ctxData.ReviewCount, intFromAny(itemMap["reviewCount"]))
			if ctxData.Section == "" {
				ctxData.Section = scalarString(itemMap["platform"])
			}
		default:
			if componentType == "SeasonList" {
				items := asSlice(dataMap["items"])
				for _, rawSeason := range items {
					seasonMap, ok := asMap(rawSeason)
					if !ok {
						continue
					}
					label := firstNonEmpty(scalarString(seasonMap["title"]), scalarString(seasonMap["label"]))
					if label == "" {
						continue
					}
					ctxData.Seasons = append(ctxData.Seasons, ReviewSeasonContext{
						Label: label,
						Href:  scalarString(seasonMap["link"]),
					})
				}
			}
		}
	}

	if ctxData.Title == "" {
		ctxData.Title = work.Name
	}
	if len(ctxData.Platforms) == 0 && work.Category == domain.CategoryGame {
		ctxData.Platforms = []ReviewPlatformContext{{Key: "", Name: ""}}
	}

	return ctxData, nil
}

type ReviewListPage struct {
	TotalResults int
	Items        []domain.ReviewRecord
	RawPayload   string
}

func (a *ReviewListAPI) FetchPage(ctx context.Context, work domain.Work, reviewType domain.ReviewType, sentiment domain.ReviewSentiment, sort domain.ReviewSort, platformKey string, offset int, limit int) (ReviewListPage, error) {
	reqURL, err := buildReviewListURL(a.baseURL, work.Category, reviewType, sentiment, sort, work.Href, platformKey, offset, limit)
	if err != nil {
		return ReviewListPage{}, err
	}

	body, err := a.fetch(ctx, reqURL)
	if err != nil {
		return ReviewListPage{}, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return ReviewListPage{}, fmt.Errorf("decode review list response: %w", err)
	}

	dataMap, _ := asMap(raw["data"])
	if dataMap == nil {
		return ReviewListPage{}, fmt.Errorf("review list response missing data")
	}

	totalResults := intFromAny(dataMap["totalResults"])
	items := asSlice(dataMap["items"])
	result := make([]domain.ReviewRecord, 0, len(items))
	for _, item := range items {
		itemMap, ok := asMap(item)
		if !ok {
			continue
		}
		record := buildReviewRecord(work, reviewType, platformKey, itemMap)
		if record.ReviewKey == "" {
			continue
		}
		result = append(result, record)
	}

	normalized, _ := json.Marshal(dataMap)
	return ReviewListPage{
		TotalResults: totalResults,
		Items:        result,
		RawPayload:   string(normalized),
	}, nil
}

func (a *ReviewPageAPI) fetch(ctx context.Context, reqURL string) ([]byte, error) {
	return fetchJSONWithRetries(ctx, a.client, reqURL, a.maxRetries)
}

func (a *ReviewListAPI) fetch(ctx context.Context, reqURL string) ([]byte, error) {
	return fetchJSONWithRetries(ctx, a.client, reqURL, a.maxRetries)
}

func fetchJSONWithRetries(ctx context.Context, client *http.Client, reqURL string, maxRetries int) ([]byte, error) {
	if maxRetries < 0 {
		maxRetries = 0
	}

	var lastErr error
	attempts := maxRetries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		setDefaultHeaders(req)

		resp, err := client.Do(req)
		if err == nil && resp != nil && resp.StatusCode < 400 {
			defer resp.Body.Close()
			return io.ReadAll(resp.Body)
		}

		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
			_ = resp.Body.Close()
			if err == nil {
				err = fmt.Errorf("metacritic api request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
			}
			lastErr = err
			if !shouldRetryHTTPStatus(resp.StatusCode) || attempt == attempts {
				return nil, err
			}
		} else if err != nil {
			lastErr = err
			if attempt == attempts {
				return nil, err
			}
		}

		delay := time.Duration(attempt) * 750 * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("metacritic api request failed")
	}
	return nil, lastErr
}

func shouldRetryHTTPStatus(status int) bool {
	return status == 429 || (status >= 500 && status <= 599)
}

func setDefaultHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Referer", "https://www.metacritic.com/")
}

func buildReviewPageURL(baseURL string, category domain.Category, reviewType domain.ReviewType, workHref string) (string, error) {
	slug := slugFromWorkHref(category, workHref)
	if slug == "" {
		return "", fmt.Errorf("review page slug is empty")
	}

	section, err := reviewSectionPath(category)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/composer/metacritic/pages/%s-%s-reviews/%s/web", strings.TrimRight(baseURL, "/"), section, reviewType, slug), nil
}

func buildReviewListURL(baseURL string, category domain.Category, reviewType domain.ReviewType, sentiment domain.ReviewSentiment, sort domain.ReviewSort, workHref string, platformKey string, offset int, limit int) (string, error) {
	slug := slugFromWorkHref(category, workHref)
	if slug == "" {
		return "", fmt.Errorf("review list slug is empty")
	}

	section, err := reviewSectionPath(category)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "reviews", "metacritic", string(reviewType), section, slug, "web")
	q := u.Query()
	if category == domain.CategoryGame && strings.TrimSpace(platformKey) != "" {
		q.Set("platform", strings.TrimSpace(platformKey))
	}
	if sentiment != "" && sentiment != domain.ReviewSentimentAll {
		q.Set("filterBySentiment", string(sentiment))
	}
	if sort != "" {
		q.Set("sort", string(sort))
	}
	if offset >= 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func reviewSectionPath(category domain.Category) (string, error) {
	switch category {
	case domain.CategoryGame:
		return "games", nil
	case domain.CategoryMovie:
		return "movies", nil
	case domain.CategoryTV:
		return "shows", nil
	default:
		return "", fmt.Errorf("unsupported review category %q", category)
	}
}

func slugFromWorkHref(category domain.Category, workHref string) string {
	trimmed := strings.TrimSpace(workHref)
	if trimmed == "" {
		return ""
	}
	trimmed = strings.TrimRight(trimmed, "/")
	if idx := strings.Index(trimmed, fmt.Sprintf("/%s/", string(category))); idx >= 0 {
		return strings.TrimPrefix(trimmed[idx+len(category)+2:], "/")
	}
	if u, err := url.Parse(trimmed); err == nil {
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(parts) >= 2 {
			return strings.Join(parts[1:], "/")
		}
	}
	return ""
}

// Review field normalization contract:
//   - Shared fields always map into the common columns: review_key, work_href,
//     category, review_type, platform_key, review_url, review_date, score, quote.
//   - Critic payloads treat author/authorSlug as author_name/author_slug and
//     publication* as critic-only metadata.
//   - User payloads treat author/authorSlug as username/user_slug. We keep
//     author_name/author_slug empty for user reviews so the normalized columns
//     stay semantically stable.
//   - publication* never backfills user rows, and season_label only persists when
//     the payload explicitly exposes a season/show label.
//   - source_payload_json remains the only heterogenous fallback for fields that
//     do not fit the normalized schema.
func buildReviewRecord(work domain.Work, reviewType domain.ReviewType, requestedPlatform string, itemMap map[string]any) domain.ReviewRecord {
	score := floatPointer(itemMap["score"])
	quote := scalarString(itemMap["quote"])
	reviewURL := normalizeURL(scalarString(itemMap["url"]))
	reviewDate := firstNonEmpty(scalarString(itemMap["date"]), scalarString(itemMap["reviewDate"]))
	externalID := scalarString(itemMap["id"])
	author := scalarString(itemMap["author"])
	authorSlug := scalarString(itemMap["authorSlug"])
	publicationName := scalarString(itemMap["publicationName"])
	publicationSlug := scalarString(itemMap["publicationSlug"])
	seasonLabel := scalarString(itemMap["season"])
	versionLabel := firstNonEmpty(scalarString(itemMap["version"]), scalarString(itemMap["versionLabel"]))
	spoiler := boolPointer(itemMap["spoiler"])
	thumbsUp := int64Pointer(itemMap["thumbsUp"])
	thumbsDown := int64Pointer(itemMap["thumbsDown"])
	platformKey := firstNonEmpty(strings.TrimSpace(requestedPlatform), extractPlatformKey(itemMap))

	record := domain.ReviewRecord{
		ExternalReviewID:  externalID,
		WorkHref:          work.Href,
		Category:          work.Category,
		ReviewType:        reviewType,
		PlatformKey:       platformKey,
		ReviewURL:         reviewURL,
		ReviewDate:        reviewDate,
		Score:             score,
		Quote:             quote,
		SeasonLabel:       seasonLabel,
		ThumbsUp:          thumbsUp,
		ThumbsDown:        thumbsDown,
		VersionLabel:      versionLabel,
		SpoilerFlag:       spoiler,
		SourcePayloadJSON: marshalAny(itemMap),
	}

	switch reviewType {
	case domain.ReviewTypeUser:
		record.Username = author
		record.UserSlug = authorSlug
		record.ReviewKey = domain.BuildUserReviewKey(work.Href, work.Category, platformKey, record.ExternalReviewID, record.Username, record.ReviewDate, record.Score, record.Quote)
	default:
		record.PublicationName = publicationName
		record.PublicationSlug = publicationSlug
		record.AuthorName = author
		record.AuthorSlug = authorSlug
		record.ReviewKey = domain.BuildCriticReviewKey(work.Href, work.Category, platformKey, firstNonEmpty(record.PublicationSlug, record.AuthorSlug), record.ReviewDate, record.Quote)
	}
	return record
}

func extractPlatforms(platforms []any) []ReviewPlatformContext {
	result := make([]ReviewPlatformContext, 0, len(platforms))
	for _, raw := range platforms {
		m, ok := asMap(raw)
		if !ok {
			continue
		}
		result = append(result, ReviewPlatformContext{
			Key:         firstNonEmpty(scalarString(m["slug"]), scalarString(m["key"])),
			Name:        firstNonEmpty(scalarString(m["name"]), scalarString(m["title"])),
			ReviewCount: intFromAny(m["reviewCount"]),
		})
	}
	return result
}

func extractPlatformKey(itemMap map[string]any) string {
	if platform, ok := asMap(itemMap["platform"]); ok {
		return firstNonEmpty(scalarString(platform["slug"]), scalarString(platform["name"]))
	}
	if reviewedProduct, ok := asMap(itemMap["reviewedProduct"]); ok {
		if platform, ok := asMap(reviewedProduct["platform"]); ok {
			return firstNonEmpty(scalarString(platform["slug"]), scalarString(platform["name"]))
		}
	}
	return ""
}

func normalizeURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return trimmed
	}
	return "https://www.metacritic.com" + trimmed
}

func marshalAny(v any) string {
	content, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	if bytes.Equal(content, []byte("null")) {
		return "{}"
	}
	return string(content)
}

func scalarString(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func intFromAny(v any) int {
	switch typed := v.(type) {
	case float64:
		return int(typed)
	case json.Number:
		n, _ := typed.Int64()
		return int(n)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(typed))
		return n
	default:
		return 0
	}
}

func floatPointer(v any) *float64 {
	switch typed := v.(type) {
	case float64:
		return &typed
	case json.Number:
		f, err := typed.Float64()
		if err == nil {
			return &f
		}
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil {
			return &f
		}
	}
	return nil
}

func int64Pointer(v any) *int64 {
	switch typed := v.(type) {
	case float64:
		n := int64(typed)
		return &n
	case json.Number:
		n, err := typed.Int64()
		if err == nil {
			return &n
		}
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if err == nil {
			return &n
		}
	}
	return nil
}

func boolPointer(v any) *bool {
	switch typed := v.(type) {
	case bool:
		return &typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1":
			value := true
			return &value
		case "false", "0":
			value := false
			return &value
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func asSlice(v any) []any {
	switch typed := v.(type) {
	case []any:
		return typed
	default:
		return nil
	}
}

func maxInt(a, b int) int {
	if b > a {
		return b
	}
	return a
}
