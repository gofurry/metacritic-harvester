package metacritic

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"

	"github.com/gofurry/metacritic-harvester/internal/domain"
)

func parseNuxtDetail(category domain.Category, workHref string, doc *goquery.Document, detail *domain.WorkDetail) error {
	raw, found, err := ExtractNuxtData(doc)
	if err != nil {
		return detailFieldError(category, workHref, "nuxt_data", "parse", err.Error())
	}
	if !found {
		return nil
	}

	root, err := parseNuxtRoot(raw)
	if err != nil {
		return detailFieldError(category, workHref, "nuxt_data", "parse", fmt.Sprintf("decode __NUXT_DATA__: %v", err))
	}

	switch category {
	case domain.CategoryGame:
		options, ok, parseErr := ParseWhereToBuy(root)
		if parseErr != nil {
			return detailFieldError(category, workHref, "where_to_buy", "parse", parseErr.Error())
		}
		if ok {
			detail.Details.WhereToBuy = options
		}
	case domain.CategoryMovie, domain.CategoryTV:
		groups, ok, parseErr := ParseWhereToWatch(root)
		if parseErr != nil {
			return detailFieldError(category, workHref, "where_to_watch", "parse", parseErr.Error())
		}
		if ok {
			detail.Details.WhereToWatch = groups
		}
	}

	return nil
}

func ExtractNuxtData(doc *goquery.Document) (string, bool, error) {
	selection := doc.Find("script#__NUXT_DATA__").First()
	if selection.Length() == 0 {
		return "", false, nil
	}

	raw := strings.TrimSpace(selection.Text())
	if raw == "" {
		return "", true, fmt.Errorf("__NUXT_DATA__ is empty")
	}
	return raw, true, nil
}

func parseNuxtRoot(raw string) ([]any, error) {
	var root []any
	if err := json.Unmarshal([]byte(raw), &root); err != nil {
		return nil, err
	}
	return root, nil
}

func ResolveNuxtValue(root []any, value any) any {
	return resolveNuxtValue(root, value, map[int]bool{})
}

func resolveNuxtValue(root []any, value any, visiting map[int]bool) any {
	if index, ok := nuxtIndex(value); ok {
		if index < 0 || index >= len(root) || visiting[index] {
			return value
		}
		visiting[index] = true
		defer delete(visiting, index)
		return resolveNuxtValue(root, root[index], visiting)
	}

	switch typed := value.(type) {
	case []any:
		out := make([]any, len(typed))
		for i := range typed {
			out[i] = resolveNuxtValue(root, typed[i], visiting)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = resolveNuxtValue(root, item, visiting)
		}
		return out
	default:
		return value
	}
}

func ParseWhereToBuy(root []any) ([]domain.BuyOption, bool, error) {
	var results []domain.BuyOption
	seen := make(map[string]bool)

	for _, item := range root {
		group, ok := nuxtMap(root, item, map[int]bool{})
		if !ok {
			continue
		}
		if _, hasGroupName := group["groupName"]; !hasGroupName {
			continue
		}
		if _, hasGroupOptions := group["groupOptions"]; !hasGroupOptions {
			continue
		}

		groupName := nuxtString(root, group["groupName"], map[int]bool{})
		groupOptions, ok := nuxtSlice(root, group["groupOptions"], map[int]bool{})
		if !ok || groupName == "" {
			continue
		}

		for _, item := range groupOptions {
			provider, ok := nuxtMap(root, item, map[int]bool{})
			if !ok {
				continue
			}

			store := firstNonEmpty(
				nuxtString(root, provider["name"], map[int]bool{}),
				nuxtString(root, provider["store"], map[int]bool{}),
			)
			lowestPrice := nuxtFloatPointer(root, provider["lowestPriceOption"], "price")
			purchaseOptions, ok := nuxtSlice(root, provider["purchaseOptions"], map[int]bool{})
			if !ok || len(purchaseOptions) == 0 {
				option := buildBuyOption(root, groupName, store, lowestPrice, provider)
				if addBuyOption(&results, seen, option) {
					continue
				}
				continue
			}

			for _, rawOption := range purchaseOptions {
				optionMap, ok := nuxtMap(root, rawOption, map[int]bool{})
				if !ok {
					continue
				}
				option := buildBuyOption(root, groupName, store, lowestPrice, optionMap)
				addBuyOption(&results, seen, option)
			}
		}
	}

	return results, len(results) > 0, nil
}

func ParseWhereToWatch(root []any) ([]domain.WatchGroup, bool, error) {
	var results []domain.WatchGroup
	seen := make(map[string]bool)

	for _, item := range root {
		container, ok := nuxtMap(root, item, map[int]bool{})
		if !ok || len(container) == 0 {
			continue
		}
		if _, hasOffers := container["hasOffers"]; !hasOffers {
			continue
		}

		for _, groupName := range []string{"free", "rent", "buy", "flatrate", "ads"} {
			rawProviders, ok := container[groupName]
			if !ok {
				continue
			}
			providers, ok := nuxtSlice(root, rawProviders, map[int]bool{})
			if !ok || len(providers) == 0 {
				continue
			}

			for _, rawProvider := range providers {
				provider, ok := nuxtMap(root, rawProvider, map[int]bool{})
				if !ok {
					continue
				}

				watchGroup := domain.WatchGroup{
					GroupName:          groupName,
					ProviderName:       nuxtString(root, provider["providerName"], map[int]bool{}),
					ProviderID:         nuxtScalarString(root, provider["providerId"], map[int]bool{}),
					ProviderIcon:       firstNonEmpty(nuxtString(root, provider["providerIcon"], map[int]bool{}), nuxtString(root, provider["icon"], map[int]bool{})),
					LinkURL:            firstNonEmpty(nuxtString(root, provider["link"], map[int]bool{}), nuxtString(root, provider["linkUrl"], map[int]bool{})),
					Monetization:       firstNonEmpty(nuxtString(root, provider["monetization"], map[int]bool{}), groupName),
					OfferType:          nuxtString(root, provider["offerType"], map[int]bool{}),
					QualityType:        nuxtString(root, provider["qualityType"], map[int]bool{}),
					OptionCurrency:     nuxtString(root, provider["optionCurrency"], map[int]bool{}),
					OptionCurrencyCode: nuxtString(root, provider["optionCurrencyCode"], map[int]bool{}),
					NumberOfSeasons:    nuxtInt(root, provider["numberOfSeasons"], map[int]bool{}),
				}

				options, ok := nuxtSlice(root, provider["options"], map[int]bool{})
				if ok && len(options) > 0 {
					for _, rawOption := range options {
						optionMap, ok := nuxtMap(root, rawOption, map[int]bool{})
						if !ok {
							continue
						}
						option := domain.WatchOption{
							OfferType:          firstNonEmpty(nuxtString(root, optionMap["offerType"], map[int]bool{}), watchGroup.OfferType),
							QualityType:        firstNonEmpty(nuxtString(root, optionMap["qualityType"], map[int]bool{}), watchGroup.QualityType),
							Monetization:       firstNonEmpty(nuxtString(root, optionMap["monetization"], map[int]bool{}), watchGroup.Monetization),
							LinkURL:            firstNonEmpty(nuxtString(root, optionMap["link"], map[int]bool{}), nuxtString(root, optionMap["linkUrl"], map[int]bool{}), watchGroup.LinkURL),
							OptionCurrency:     firstNonEmpty(nuxtString(root, optionMap["optionCurrency"], map[int]bool{}), watchGroup.OptionCurrency),
							OptionCurrencyCode: firstNonEmpty(nuxtString(root, optionMap["optionCurrencyCode"], map[int]bool{}), watchGroup.OptionCurrencyCode),
							OptionPrice:        nuxtFloat(root, optionMap["optionPrice"], map[int]bool{}),
						}
						watchGroup.Options = append(watchGroup.Options, option)
					}
				}

				if watchGroup.OfferType == "" && len(watchGroup.Options) > 0 {
					watchGroup.OfferType = watchGroup.Options[0].OfferType
				}
				if watchGroup.QualityType == "" && len(watchGroup.Options) > 0 {
					watchGroup.QualityType = watchGroup.Options[0].QualityType
				}
				if watchGroup.OptionCurrency == "" && len(watchGroup.Options) > 0 {
					watchGroup.OptionCurrency = watchGroup.Options[0].OptionCurrency
				}
				if watchGroup.OptionCurrencyCode == "" && len(watchGroup.Options) > 0 {
					watchGroup.OptionCurrencyCode = watchGroup.Options[0].OptionCurrencyCode
				}

				key := strings.Join([]string{watchGroup.GroupName, watchGroup.ProviderName, watchGroup.LinkURL}, "|")
				if key == "||" || seen[key] {
					continue
				}
				seen[key] = true
				results = append(results, watchGroup)
			}
		}
	}

	return results, len(results) > 0, nil
}

func buildBuyOption(root []any, groupName string, fallbackStore string, lowestPrice *float64, option map[string]any) domain.BuyOption {
	return domain.BuyOption{
		GroupName:          groupName,
		Store:              firstNonEmpty(fallbackStore, nuxtString(root, option["store"], map[int]bool{}), nuxtString(root, option["name"], map[int]bool{})),
		LinkURL:            firstNonEmpty(nuxtString(root, option["linkUrl"], map[int]bool{}), nuxtString(root, option["link"], map[int]bool{})),
		Price:              nuxtFloat(root, option["price"], map[int]bool{}),
		OriginalPrice:      nuxtFloat(root, option["originalPrice"], map[int]bool{}),
		DiscountedPrice:    nuxtFloat(root, option["discountedPrice"], map[int]bool{}),
		DiscountPercentage: nuxtFloat(root, option["discountPercentage"], map[int]bool{}),
		ImageURL:           firstNonEmpty(nuxtString(root, option["image"], map[int]bool{}), nuxtString(root, option["imageUrl"], map[int]bool{})),
		PurchaseType:       nuxtString(root, option["purchaseType"], map[int]bool{}),
		LowestPrice:        lowestPrice,
	}
}

func addBuyOption(target *[]domain.BuyOption, seen map[string]bool, option domain.BuyOption) bool {
	key := strings.Join([]string{option.GroupName, option.Store, option.LinkURL}, "|")
	if key == "||" || seen[key] {
		return false
	}
	seen[key] = true
	*target = append(*target, option)
	return true
}

func nuxtMap(root []any, value any, visiting map[int]bool) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	default:
		resolved, ok := resolveNuxtRef(root, value, visiting)
		if !ok {
			return nil, false
		}
		return nuxtMap(root, resolved, visiting)
	}
}

func nuxtSlice(root []any, value any, visiting map[int]bool) ([]any, bool) {
	switch typed := value.(type) {
	case []any:
		return typed, true
	default:
		resolved, ok := resolveNuxtRef(root, value, visiting)
		if !ok {
			return nil, false
		}
		return nuxtSlice(root, resolved, visiting)
	}
}

func nuxtString(root []any, value any, visiting map[int]bool) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		resolved, ok := resolveNuxtRef(root, value, visiting)
		if !ok {
			return ""
		}
		return nuxtString(root, resolved, visiting)
	}
}

func nuxtScalarString(root []any, value any, visiting map[int]bool) string {
	if raw, ok := scalarNuxtValue(root, value, visiting); ok {
		switch typed := raw.(type) {
		case string:
			return typed
		case float64:
			if math.Trunc(typed) == typed {
				return strconv.Itoa(int(typed))
			}
			return strconv.FormatFloat(typed, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(typed)
		}
	}
	return ""
}

func nuxtFloat(root []any, value any, visiting map[int]bool) *float64 {
	switch typed := value.(type) {
	case float64:
		if math.Trunc(typed) != typed {
			return float64Ptr(typed)
		}
		resolved, ok := resolveNuxtRef(root, value, visiting)
		if ok {
			if candidate, ok := scalarNuxtFloat(root, resolved, visiting); ok {
				return float64Ptr(candidate)
			}
		}
		return float64Ptr(typed)
	default:
		if candidate, ok := scalarNuxtFloat(root, value, visiting); ok {
			return float64Ptr(candidate)
		}
		return nil
	}
}

func nuxtFloatPointer(root []any, value any, field string) *float64 {
	mapValue, ok := nuxtMap(root, value, map[int]bool{})
	if !ok {
		return nil
	}
	return nuxtFloat(root, mapValue[field], map[int]bool{})
}

func nuxtInt(root []any, value any, visiting map[int]bool) int {
	if scalar, ok := scalarNuxtValue(root, value, visiting); ok {
		switch typed := scalar.(type) {
		case float64:
			if math.Trunc(typed) == typed {
				return int(typed)
			}
		case string:
			if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil {
				return parsed
			}
		}
	}
	return 0
}

func scalarNuxtFloat(root []any, value any, visiting map[int]bool) (float64, bool) {
	if raw, ok := scalarNuxtValue(root, value, visiting); ok {
		switch typed := raw.(type) {
		case float64:
			return typed, true
		case string:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(typed, "%")), 64)
			if err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

func scalarNuxtValue(root []any, value any, visiting map[int]bool) (any, bool) {
	switch typed := value.(type) {
	case string, bool:
		return typed, true
	case float64:
		if math.Trunc(typed) != typed {
			return typed, true
		}
		resolved, ok := resolveNuxtRef(root, value, visiting)
		if !ok {
			return typed, true
		}
		switch resolvedTyped := resolved.(type) {
		case string, bool, float64:
			return resolvedTyped, true
		default:
			return typed, true
		}
	default:
		resolved, ok := resolveNuxtRef(root, value, visiting)
		if !ok {
			return nil, false
		}
		return scalarNuxtValue(root, resolved, visiting)
	}
}

func resolveNuxtRef(root []any, value any, visiting map[int]bool) (any, bool) {
	index, ok := nuxtIndex(value)
	if !ok || index < 0 || index >= len(root) || visiting[index] {
		return nil, false
	}
	visiting[index] = true
	defer delete(visiting, index)
	return root[index], true
}

func nuxtIndex(value any) (int, bool) {
	floatValue, ok := value.(float64)
	if !ok || math.Trunc(floatValue) != floatValue {
		return 0, false
	}
	index := int(floatValue)
	if float64(index) != floatValue {
		return 0, false
	}
	return index, true
}

func copyNuxtVisiting(visiting map[int]bool) map[int]bool {
	out := make(map[int]bool, len(visiting))
	for key, value := range visiting {
		out[key] = value
	}
	return out
}

func float64Ptr(value float64) *float64 {
	return &value
}
