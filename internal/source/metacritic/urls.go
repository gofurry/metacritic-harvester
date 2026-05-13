package metacritic

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/gofurry/metacritic-harvester/internal/config"
	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func BuildListURL(category domain.Category, metric domain.Metric, filter domain.Filter, page int) string {
	return BuildListURLWithBase(config.DefaultBaseURL, category, metric, filter, page)
}

func BuildListURLWithBase(base string, category domain.Category, metric domain.Metric, filter domain.Filter, page int) string {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" {
		base = config.DefaultBaseURL
	}

	var path string
	switch metric {
	case domain.MetricMetascore:
		path = fmt.Sprintf("/browse/%s/", category)
	case domain.MetricUserScore:
		path = fmt.Sprintf("/browse/%s/all/all/all-time/userscore/", category)
	case domain.MetricNewest:
		path = fmt.Sprintf("/browse/%s/all/all/all-time/new/", category)
	default:
		path = fmt.Sprintf("/browse/%s/", category)
	}

	u, _ := url.Parse(base + path)
	q := u.Query()

	if filter.ReleaseYearMin != nil {
		q.Set("releaseYearMin", strconv.Itoa(*filter.ReleaseYearMin))
	}
	if filter.ReleaseYearMax != nil {
		q.Set("releaseYearMax", strconv.Itoa(*filter.ReleaseYearMax))
	}

	for _, platform := range filter.Platforms {
		platform = strings.TrimSpace(platform)
		if platform != "" {
			q.Add("platform", platform)
		}
	}
	for _, network := range filter.Networks {
		network = strings.TrimSpace(network)
		if network != "" {
			q.Add("network", network)
		}
	}
	for _, genre := range filter.Genres {
		genre = strings.TrimSpace(genre)
		if genre != "" {
			q.Add("genre", genre)
		}
	}
	for _, releaseType := range filter.ReleaseTypes {
		releaseType = strings.TrimSpace(releaseType)
		if releaseType != "" {
			q.Add("releaseType", releaseType)
		}
	}

	if page > 1 {
		q.Set("page", fmt.Sprintf("%d", page))
	}
	u.RawQuery = q.Encode()
	return u.String()
}
