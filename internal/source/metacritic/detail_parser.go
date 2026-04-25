package metacritic

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"

	"github.com/GoFurry/metacritic-harvester/internal/domain"
)

var countRe = regexp.MustCompile(`\d[\d,]*`)

func ParseDetail(category domain.Category, workHref string, body io.Reader) (domain.WorkDetail, error) {
	doc, err := goquery.NewDocumentFromReader(body)
	if err != nil {
		return domain.WorkDetail{}, fmt.Errorf("parse detail html: %w", err)
	}
	return ParseDetailDocument(category, workHref, doc)
}

func ParseDetailDocument(category domain.Category, workHref string, doc *goquery.Document) (domain.WorkDetail, error) {
	detail := domain.WorkDetail{
		WorkHref:      toAbsoluteURL(workHref),
		Category:      category,
		LastFetchedAt: time.Now().UTC(),
	}

	switch category {
	case domain.CategoryGame:
		parseGameDetail(doc, &detail)
	case domain.CategoryMovie:
		parseMovieDetail(doc, &detail)
	case domain.CategoryTV:
		parseTVDetail(doc, &detail)
	default:
		return domain.WorkDetail{}, fmt.Errorf("unsupported detail category %q", category)
	}

	if err := parseNuxtDetail(category, workHref, doc, &detail); err != nil {
		return domain.WorkDetail{}, err
	}

	if err := validateDetail(category, workHref, detail); err != nil {
		return domain.WorkDetail{}, err
	}
	return detail, nil
}

func EnrichDetail(category domain.Category, workHref string, body io.Reader, detail *domain.WorkDetail) error {
	doc, err := goquery.NewDocumentFromReader(body)
	if err != nil {
		return fmt.Errorf("parse detail enrich html: %w", err)
	}
	return EnrichDetailDocument(category, workHref, doc, detail)
}

func EnrichDetailDocument(category domain.Category, workHref string, doc *goquery.Document, detail *domain.WorkDetail) error {
	if detail == nil {
		return fmt.Errorf("detail enrich target is nil")
	}
	return parseNuxtDetail(category, workHref, doc, detail)
}

func parseGameDetail(doc *goquery.Document, detail *domain.WorkDetail) {
	detail.Title = firstText(doc.Selection, "h1.hero-title__text, h1")
	detail.Summary = cleanTextWithout(doc.Find(".c-game-details__summary-description").First(), ".c-game-details__read-more")
	detail.ReleaseDate = firstNonEmpty(
		firstText(doc.Selection, ".product-hero__release-date .hero-release-date__value"),
		firstDetailValue(doc, "Initial Release Date"),
	)
	detail.Rating = firstText(doc.Selection, ".c-game-details__esrb-title")

	detail.Details.CurrentPlatform = firstText(doc.Selection, ".product-hero svg.game-platform-logo__icon title, svg.game-platform-logo__icon title")
	detail.Details.ESRBRating = detail.Rating
	detail.Details.ESRBDescription = firstText(doc.Selection, ".c-game-details__esrb-subtitle")
	detail.Details.Platforms = detailValues(doc, "Platforms")
	detail.Details.Developers = detailValues(doc, "Developer")
	detail.Details.Publishers = detailValues(doc, "Publisher")
	detail.Details.Genres = detailValues(doc, "Genres")
	detail.Details.PlatformScores = parsePlatformScores(doc)
}

func parseMovieDetail(doc *goquery.Document, detail *domain.WorkDetail) {
	detail.Title = firstText(doc.Selection, "h1.hero-title__text, h1")
	parseGlobalScores(doc, detail)
	detail.Summary = cleanTextWithout(doc.Find(".c-hero-summary__description").First(), ".c-hero-summary__read-more")
	detail.ReleaseDate = firstDetailValue(doc, "Release Date")
	detail.Duration = firstDetailValue(doc, "Duration")
	detail.Rating = firstDetailValue(doc, "Rating")
	detail.Tagline = firstDetailValue(doc, "Tagline")

	detail.Details.Genres = detailValues(doc, "Genres")
	if len(detail.Details.Genres) == 0 {
		detail.Details.Genres = texts(doc.Find(".c-hero-summary .c-genreList_item .global-link-button__label"))
	}
	detail.Details.Directors = crewValues(doc, "Directed By")
	detail.Details.Writers = crewValues(doc, "Written By")
	detail.Details.ProductionCompanies = detailValues(doc, "Production Company")
	detail.Details.Awards = awardValues(doc)
}

func parseTVDetail(doc *goquery.Document, detail *domain.WorkDetail) {
	detail.Title = firstText(doc.Selection, "h1.hero-title__text, h1")
	parseGlobalScores(doc, detail)
	detail.Summary = cleanTextWithout(doc.Find(".c-hero-summary__description").First(), ".c-hero-summary__read-more")
	detail.ReleaseDate = firstDetailValue(doc, "Initial Release Date")
	detail.Rating = firstDetailValue(doc, "Rating")

	detail.Details.Genres = detailValues(doc, "Genres")
	if len(detail.Details.Genres) == 0 {
		detail.Details.Genres = texts(doc.Find(".c-hero-summary .c-genreList_item .global-link-button__label"))
	}
	detail.Details.ProductionCompanies = detailValues(doc, "Production Company")
	detail.Details.NumberOfSeasons = firstDetailValue(doc, "Number of seasons")
	detail.Details.Seasons = seasonValues(doc)
	detail.Details.Awards = awardValues(doc)
}

func parseGlobalScores(doc *goquery.Document, detail *domain.WorkDetail) {
	doc.Find(`[data-testid="global-score-wrapper"]`).Each(func(_ int, score *goquery.Selection) {
		header := strings.ToLower(cleanText(score.Find(`[data-testid="global-score-header"]`).First().Text()))
		value := firstText(score, `[data-testid="global-score-value"]`)
		sentiment := firstText(score, `[data-testid="global-score-sentiment"]`)
		count := parseCount(firstText(score, `[data-testid="global-score-review-count"]`))

		switch header {
		case "metascore":
			detail.Metascore = value
			detail.MetascoreSentiment = sentiment
			detail.MetascoreReviewCount = count
		case "user score":
			detail.UserScore = value
			detail.UserScoreSentiment = sentiment
			detail.UserScoreCount = count
		}
	})
}

func parsePlatformScores(doc *goquery.Document) []domain.PlatformScore {
	var result []domain.PlatformScore
	doc.Find(`.game-platforms[data-testid="all-platforms"] a[data-testid="product-score-card"]`).Each(func(_ int, card *goquery.Selection) {
		href, _ := card.Attr("href")
		score := domain.PlatformScore{
			Platform:          firstText(card, "svg.game-platform-logo__icon title, svg title"),
			Href:              toAbsoluteURL(href),
			Metascore:         firstScore(card),
			CriticReviewCount: parseCount(firstText(card, ".product-score-card__review-count")),
		}
		if score.Platform != "" || score.Href != "" || score.Metascore != "" {
			result = append(result, score)
		}
	})
	return result
}

func seasonValues(doc *goquery.Document) []domain.SeasonSummary {
	var result []domain.SeasonSummary
	doc.Find(`.tv-seasons[data-testid="all-seasons"] a[data-testid="product-score-card"]`).Each(func(_ int, card *goquery.Selection) {
		href, _ := card.Attr("href")
		meta := texts(card.Find(".product-score-card__season-meta span"))
		season := domain.SeasonSummary{
			Label:     firstText(card, ".product-score-card__season-label"),
			Href:      toAbsoluteURL(href),
			Metascore: firstScore(card),
		}
		for _, value := range meta {
			if strings.Contains(strings.ToLower(value), "episode") {
				season.Episodes = value
				continue
			}
			if countRe.MatchString(value) {
				season.Year = value
			}
		}
		if season.Label != "" || season.Href != "" || season.Metascore != "" {
			result = append(result, season)
		}
	})
	return result
}

func awardValues(doc *goquery.Document) []domain.AwardSummary {
	var result []domain.AwardSummary
	doc.Find(`[data-testid="details-award-summary"] .c-production-award-summary__award`).Each(func(_ int, award *goquery.Selection) {
		item := domain.AwardSummary{
			Event:   firstText(award, ".c-production-award-summary__award-event"),
			Details: strings.TrimPrefix(firstText(award, ".c-production-award-summary__award-details"), "• "),
		}
		if item.Event != "" || item.Details != "" {
			result = append(result, item)
		}
	})
	return result
}

func detailValues(doc *goquery.Document, label string) []string {
	var result []string
	normalizedLabel := normalizeLabel(label)
	doc.Find(".c-product-details__section").Each(func(_ int, section *goquery.Selection) {
		if normalizeLabel(firstText(section, ".c-product-details__section__label")) != normalizedLabel {
			return
		}
		values := texts(section.Find(".c-product-details__section__list-item, .c-genreList_item .global-link-button__label"))
		if len(values) == 0 {
			values = append(values, firstText(section, ".c-product-details__section__value"))
		}
		result = append(result, values...)
	})
	return compactStrings(result)
}

func firstDetailValue(doc *goquery.Document, label string) string {
	values := detailValues(doc, label)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func crewValues(doc *goquery.Document, label string) []string {
	var result []string
	normalizedLabel := normalizeLabel(label)
	doc.Find(".c-crew-list").Each(func(_ int, crew *goquery.Selection) {
		if normalizeLabel(firstText(crew, ".c-crew-list__title")) != normalizedLabel {
			return
		}
		result = append(result, texts(crew.Find(".c-crew-list__link"))...)
	})
	return compactStrings(result)
}

func firstScore(selection *goquery.Selection) string {
	return firstNonEmpty(
		firstText(selection, `[aria-label*="Metascore"] span`),
		firstText(selection, ".c-siteReviewScore span"),
		firstText(selection, `[data-testid="global-score-value"]`),
	)
}

func firstText(selection *goquery.Selection, selector string) string {
	return cleanText(selection.Find(selector).First().Text())
}

func cleanTextWithout(selection *goquery.Selection, removeSelectors ...string) string {
	if selection.Length() == 0 {
		return ""
	}
	clone := selection.Clone()
	for _, selector := range removeSelectors {
		clone.Find(selector).Remove()
	}
	return cleanText(clone.Text())
}

func texts(selection *goquery.Selection) []string {
	var result []string
	selection.Each(func(_ int, item *goquery.Selection) {
		value := cleanText(item.Text())
		if value != "" && value != "•" {
			result = append(result, value)
		}
	})
	return compactStrings(result)
}

func compactStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = cleanText(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func cleanText(value string) string {
	value = strings.ReplaceAll(value, "\u00a0", " ")
	return strings.Join(strings.Fields(value), " ")
}

func normalizeLabel(value string) string {
	return strings.TrimSuffix(strings.ToLower(cleanText(value)), ":")
}

func validateDetail(category domain.Category, workHref string, detail domain.WorkDetail) error {
	if detail.Title == "" {
		return detailFieldError(category, workHref, "title", "validate", "missing hero title")
	}

	return nil
}

func detailFieldError(category domain.Category, workHref string, field string, stage string, reason string) error {
	return fmt.Errorf(
		"detail parse error: category=%s field=%s stage=%s reason=%s href=%s",
		category,
		field,
		stage,
		reason,
		workHref,
	)
}

func requiredDetailFieldError(category domain.Category, workHref string, field string, reason string) error {
	return fmt.Errorf(
		"detail parse error: category=%s field=%s stage=validate reason=%s href=%s",
		category,
		field,
		reason,
		workHref,
	)
}

func parseCount(value string) int {
	raw := countRe.FindString(value)
	if raw == "" {
		return 0
	}
	count, err := strconv.Atoi(strings.ReplaceAll(raw, ",", ""))
	if err != nil {
		return 0
	}
	return count
}
