package metacritic

import (
	"regexp"
	"strings"
	"time"

	"github.com/gocolly/colly/v2"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

var dateRe = regexp.MustCompile(`\b[A-Z][a-z]{2} \d{1,2}, \d{4}\b`)

func ParseListItem(e *colly.HTMLElement, page int, rank int, task domain.ListTask) (domain.ListEntry, domain.Work, bool) {
	name := strings.TrimSpace(e.ChildText(SelectorTitle))
	if name == "" {
		return domain.ListEntry{}, domain.Work{}, false
	}

	href := toAbsoluteURL(strings.TrimSpace(e.ChildAttr("a", "href")))
	imageURL := toAbsoluteURL(strings.TrimSpace(e.ChildAttr("img", "src")))
	fullText := strings.TrimSpace(e.DOM.Text())
	releaseDate := dateRe.FindString(fullText)

	metascore := firstNonEmpty(
		strings.TrimSpace(e.DOM.Find(SelectorMetascorePrimary).First().Text()),
		firstScoreText(e, 0),
	)
	userScore := firstNonEmpty(
		strings.TrimSpace(e.DOM.Find(SelectorUserScorePrimary).First().Text()),
		firstScoreText(e, 1),
	)

	work := domain.Work{
		Name:        name,
		Href:        href,
		ImageURL:    imageURL,
		ReleaseDate: releaseDate,
		Category:    task.Category,
	}
	entry := domain.ListEntry{
		WorkHref:  href,
		Category:  task.Category,
		Metric:    task.Metric,
		Page:      page,
		Rank:      rank,
		Metascore: metascore,
		UserScore: userScore,
		FilterKey: task.Filter.Key(),
		CrawledAt: time.Now().UTC(),
	}

	return entry, work, true
}

func firstScoreText(e *colly.HTMLElement, index int) string {
	scores := e.DOM.Find(SelectorFallbackScore)
	if scores.Length() <= index {
		return ""
	}
	return strings.TrimSpace(scores.Eq(index).Text())
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func toAbsoluteURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if strings.HasPrefix(raw, "/") {
		return "https://www.metacritic.com" + raw
	}
	return raw
}
