package app

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/google/uuid"

	"github.com/GoFurry/metacritic-harvester/internal/config"
	"github.com/GoFurry/metacritic-harvester/internal/crawler"
	"github.com/GoFurry/metacritic-harvester/internal/domain"
	"github.com/GoFurry/metacritic-harvester/internal/source/metacritic"
	listapi "github.com/GoFurry/metacritic-harvester/internal/source/metacritic/api"
	"github.com/GoFurry/metacritic-harvester/internal/storage"
)

type ListServiceConfig struct {
	BaseURL        string
	BackendBaseURL string
	Source         config.CrawlSource
	DBPath         string
	Debug          bool
	MaxRetries     int
	ProxyURLs      []string
	RunSource      string
	TaskName       string
}

type ListRunResult struct {
	RunID                 string
	PagesVisited          int
	PagesScheduled        int
	PagesSucceeded        int
	PagesWritten          int
	WorksUpserted         int
	ListEntriesInserted   int
	LatestEntriesUpserted int
	Failures              int
}

type ListService struct {
	cfg ListServiceConfig
}

func NewListService(cfg ListServiceConfig) *ListService {
	return &ListService{cfg: cfg}
}

func (s *ListService) Run(ctx context.Context, task domain.ListTask) (ListRunResult, error) {
	db, err := storage.Open(ctx, s.cfg.DBPath)
	if err != nil {
		return ListRunResult{}, err
	}
	defer db.Close()

	repo := storage.NewRepository(db)
	runID := uuid.NewString()
	runSource := strings.TrimSpace(s.cfg.RunSource)
	if runSource == "" {
		runSource = "crawl list"
	}
	taskName := strings.TrimSpace(s.cfg.TaskName)
	if taskName == "" {
		taskName = fmt.Sprintf("%s-%s", task.Category, task.Metric)
	}
	startedAt := time.Now().UTC()
	if err := repo.CreateCrawlRun(ctx, runID, runSource, taskName, task, startedAt); err != nil {
		return ListRunResult{}, err
	}

	var finalErr error
	defer func() {
		finishedAt := time.Now().UTC()
		if finalErr != nil {
			_ = repo.FailCrawlRun(context.Background(), runID, finishedAt, finalErr.Error())
			return
		}
		_ = repo.CompleteCrawlRun(context.Background(), runID, finishedAt)
	}()

	source := s.normalizedSource()
	result, err := s.runListBySource(ctx, repo, task, runID, source)
	if err != nil && source == config.CrawlSourceAuto && canFallbackListToHTML(result) {
		log.Printf(
			"crawl list fallback: run_id=%s category=%s metric=%s reason=api_failed fallback_source=html error=%v",
			runID,
			task.Category,
			task.Metric,
			err,
		)
		result, err = s.runListBySource(ctx, repo, task, runID, config.CrawlSourceHTML)
	}
	if err != nil {
		finalErr = fmt.Errorf("list crawl completed with %d failure(s): %w", result.Failures, err)
		return result, finalErr
	}
	return result, nil
}

func (s *ListService) normalizedSource() config.CrawlSource {
	switch s.cfg.Source {
	case config.CrawlSourceAPI, config.CrawlSourceHTML, config.CrawlSourceAuto:
		return s.cfg.Source
	default:
		return config.CrawlSourceHTML
	}
}

func canFallbackListToHTML(result ListRunResult) bool {
	return result.WorksUpserted == 0 && result.ListEntriesInserted == 0 && result.LatestEntriesUpserted == 0 && result.PagesWritten == 0
}

func (s *ListService) runListBySource(ctx context.Context, repo *storage.Repository, task domain.ListTask, runID string, source config.CrawlSource) (ListRunResult, error) {
	switch source {
	case config.CrawlSourceAPI, config.CrawlSourceAuto:
		return s.runAPI(ctx, repo, task, runID)
	case config.CrawlSourceHTML:
		return s.runHTML(ctx, repo, task, runID)
	default:
		return ListRunResult{RunID: runID}, fmt.Errorf("unsupported list source %q", source)
	}
}

func (s *ListService) newListRunState(runID string) *listRunState {
	state := &listRunState{
		seenWorks:     make(map[string]bool),
		pageRanks:     make(map[int]int),
		pageSnapshots: make(map[string][]storage.ListEntrySnapshot),
		pageStarted:   make(map[string]bool),
		pageFinished:  make(map[string]bool),
		pageFailures:  make(map[string]error),
		result:        ListRunResult{RunID: runID},
	}
	state.setPagesScheduled(1)
	return state
}

func (s *ListService) runHTML(ctx context.Context, repo *storage.Repository, task domain.ListTask, runID string) (ListRunResult, error) {
	allowedDomains, err := allowedDomainsForBaseURL(s.cfg.BaseURL)
	if err != nil {
		return ListRunResult{RunID: runID}, err
	}

	collector, retryTracker, err := crawler.NewCollector(crawler.Config{
		AllowedDomains: allowedDomains,
		Debug:          s.cfg.Debug,
		ProxyURLs:      s.cfg.ProxyURLs,
		MaxRetries:     s.cfg.MaxRetries,
	})
	if err != nil {
		return ListRunResult{RunID: runID}, err
	}

	state := s.newListRunState(runID)
	state.retryTracker = retryTracker
	s.setupHandlers(ctx, collector, repo, task, state)

	startURL := metacritic.BuildListURLWithBase(s.cfg.BaseURL, task.Category, task.Metric, task.Filter, 1)
	failuresBeforeVisit := state.failureCount()
	if err := collector.Visit(startURL); err != nil {
		if state.failureCount() == failuresBeforeVisit {
			state.recordFailure(fmt.Errorf("visit start url: %w", err))
		}
		return state.snapshotResult(), fmt.Errorf("visit start url: %w", err)
	}

	collector.Wait()
	result := state.snapshotResult()
	if err := state.firstError(); err != nil {
		return result, err
	}
	return result, nil
}

func (s *ListService) runAPI(ctx context.Context, repo *storage.Repository, task domain.ListTask, runID string) (ListRunResult, error) {
	proxyRotator, err := crawler.NewProxyRotator(s.cfg.ProxyURLs)
	if err != nil {
		return ListRunResult{RunID: runID}, err
	}
	transport := crawler.NewHTTPTransport(s.cfg.Debug, proxyRotator)
	finder := listapi.NewFinderAPI(s.backendBaseURL(), transport, 30*time.Second, s.cfg.MaxRetries)
	state := s.newListRunState(runID)

	lastPage := task.MaxPages
	if lastPage <= 0 {
		lastPage = 1
	}

	for page := 1; page <= lastPage; page++ {
		pageURL, urlErr := listapi.BuildFinderListURLForTest(s.backendBaseURL(), task, page)
		if urlErr != nil {
			state.recordFailure(urlErr)
			return state.snapshotResult(), urlErr
		}
		if state.markPageStarted(pageURL) {
			log.Printf(
				"list page start: run_id=%s category=%s metric=%s page=%d url=%s",
				runID,
				task.Category,
				task.Metric,
				page,
				pageURL,
			)
		}

		pageData, err := finder.FetchPage(ctx, task, page)
		if err != nil {
			pageErr := fmt.Errorf("fetch finder page failed: page=%d: %w", page, err)
			state.recordFailure(pageErr)
			state.setPageFailure(pageURL, pageErr)
			if state.markPageFinished(pageURL) {
				log.Printf(
					"list page failed: run_id=%s category=%s metric=%s page=%d url=%s error=%v",
					runID,
					task.Category,
					task.Metric,
					page,
					pageURL,
					pageErr,
				)
			}
			return state.snapshotResult(), pageErr
		}

		if pageData.LastPage > 0 && page == 1 {
			lastPage = pageData.LastPage
			if task.MaxPages > 0 && task.MaxPages < lastPage {
				lastPage = task.MaxPages
			}
			state.setPagesScheduled(lastPage)
		}

		state.incrementPagesSucceeded()

		snapshots := make([]storage.ListEntrySnapshot, 0, len(pageData.Items))
		for i, item := range pageData.Items {
			entry := domain.ListEntry{
				CrawlRunID: runID,
				WorkHref:   item.Work.Href,
				Category:   task.Category,
				Metric:     task.Metric,
				Page:       page,
				Rank:       ((page - 1) * listapi.FinderPageSizeForTest()) + i + 1,
				Metascore:  item.Metascore,
				UserScore:  item.UserScore,
				FilterKey:  task.Filter.Key(),
				CrawledAt:  time.Now().UTC(),
			}
			snapshots = append(snapshots, storage.ListEntrySnapshot{
				Work:  item.Work,
				Entry: entry,
			})
		}

		if len(snapshots) > 0 {
			if err := repo.SaveListEntrySnapshots(ctx, snapshots); err != nil {
				pageErr := fmt.Errorf("save list entry snapshots: %w", err)
				state.recordFailure(pageErr)
				state.setPageFailure(pageURL, pageErr)
				if state.markPageFinished(pageURL) {
					log.Printf(
						"list page failed: run_id=%s category=%s metric=%s page=%d url=%s error=%v",
						runID,
						task.Category,
						task.Metric,
						page,
						pageURL,
						pageErr,
					)
				}
				return state.snapshotResult(), pageErr
			}
		}

		newWorks := 0
		for _, snapshot := range snapshots {
			if state.markSeen(snapshot.Work.Href) {
				newWorks++
			}
		}
		state.incrementWorksBy(newWorks)
		state.incrementEntriesBy(len(snapshots))
		state.incrementLatestEntriesBy(len(snapshots))
		state.incrementPagesWritten()
		if state.markPageFinished(pageURL) {
			log.Printf(
				"list page succeeded: run_id=%s category=%s metric=%s page=%d url=%s written=%d new_works=%d",
				runID,
				task.Category,
				task.Metric,
				page,
				pageURL,
				len(snapshots),
				newWorks,
			)
		}
	}

	result := state.snapshotResult()
	if err := state.firstError(); err != nil {
		return result, err
	}
	return result, nil
}

func (s *ListService) backendBaseURL() string {
	baseURL := strings.TrimSpace(s.cfg.BackendBaseURL)
	if baseURL == "" {
		return config.DefaultBackendBaseURL
	}
	return baseURL
}

type listRunState struct {
	mu             sync.Mutex
	retryTracker   *crawler.RetryTracker
	seenWorks      map[string]bool
	pageRanks      map[int]int
	pageSnapshots  map[string][]storage.ListEntrySnapshot
	pageStarted    map[string]bool
	pageFinished   map[string]bool
	pageFailures   map[string]error
	pagesScheduled bool
	result         ListRunResult
	firstErr       error
}

func (s *ListService) setupHandlers(ctx context.Context, c *colly.Collector, repo *storage.Repository, task domain.ListTask, state *listRunState) {
	c.OnRequest(func(r *colly.Request) {
		crawler.SetDefaultRequestHeaders(r)
		if r != nil && r.URL != nil && state.markPageStarted(r.URL.String()) {
			log.Printf(
				"list page start: run_id=%s category=%s metric=%s page=%d url=%s",
				state.result.RunID,
				task.Category,
				task.Metric,
				pageNumberFromRequest(r.URL),
				r.URL.String(),
			)
		}
		if s.cfg.Debug {
			log.Printf("visit: %s", r.URL.String())
		}
	})

	c.OnResponse(func(r *colly.Response) {
		state.retryTracker.Reset(r.Request.URL.String())
		state.incrementPagesSucceeded()
	})

	c.OnError(func(r *colly.Response, err error) {
		if r == nil || r.Request == nil {
			state.recordFailure(fmt.Errorf("request failed: %w", err))
			return
		}

		urlStr := r.Request.URL.String()
		attempt, shouldRetry := state.retryTracker.Next(urlStr)
		if !shouldRetry {
			pageErr := fmt.Errorf("request failed permanently: %s: %w", urlStr, err)
			if s.cfg.Debug {
				log.Printf("request failed permanently: %s err=%v", urlStr, err)
			}
			state.recordFailure(pageErr)
			if state.setPageFailure(urlStr, pageErr) && state.markPageFinished(urlStr) {
				log.Printf(
					"list page failed: run_id=%s category=%s metric=%s page=%d url=%s error=%v",
					state.result.RunID,
					task.Category,
					task.Metric,
					pageNumberFromRequest(r.Request.URL),
					urlStr,
					pageErr,
				)
			}
			return
		}

		if s.cfg.Debug {
			log.Printf("request failed: %s err=%v retry=%d/%d", urlStr, err, attempt, s.cfg.MaxRetries)
		}
		time.Sleep(500 * time.Millisecond)
		if retryErr := r.Request.Retry(); retryErr != nil {
			state.recordFailure(fmt.Errorf("retry request %s: %w", urlStr, retryErr))
		}
	})

	c.OnHTML(metacritic.SelectorCard, func(e *colly.HTMLElement) {
		page := pageNumberFromRequest(e.Request.URL)
		rank := state.nextRank(page)

		entry, work, ok := metacritic.ParseListItem(e, page, rank, task)
		if !ok {
			pageErr := fmt.Errorf("parse list item failed: %s", e.Request.URL.String())
			state.recordFailure(pageErr)
			state.setPageFailure(e.Request.URL.String(), pageErr)
			return
		}
		entry.CrawlRunID = state.result.RunID
		state.appendPageSnapshot(e.Request.URL.String(), storage.ListEntrySnapshot{Work: work, Entry: entry})
	})

	c.OnScraped(func(r *colly.Response) {
		if r == nil || r.Request == nil {
			return
		}

		snapshots := state.takePageSnapshots(r.Request.URL.String())
		if len(snapshots) == 0 {
			if pageErr := state.pageFailure(r.Request.URL.String()); pageErr != nil && state.markPageFinished(r.Request.URL.String()) {
				log.Printf(
					"list page failed: run_id=%s category=%s metric=%s page=%d url=%s error=%v",
					state.result.RunID,
					task.Category,
					task.Metric,
					pageNumberFromRequest(r.Request.URL),
					r.Request.URL.String(),
					pageErr,
				)
			}
			return
		}

		if err := repo.SaveListEntrySnapshots(ctx, snapshots); err != nil {
			pageErr := fmt.Errorf("save list entry snapshots: %w", err)
			state.recordFailure(pageErr)
			state.setPageFailure(r.Request.URL.String(), pageErr)
			if s.cfg.Debug {
				log.Printf("save list entry snapshots failed: %v", err)
			}
			if state.markPageFinished(r.Request.URL.String()) {
				log.Printf(
					"list page failed: run_id=%s category=%s metric=%s page=%d url=%s error=%v",
					state.result.RunID,
					task.Category,
					task.Metric,
					pageNumberFromRequest(r.Request.URL),
					r.Request.URL.String(),
					pageErr,
				)
			}
			return
		}

		newWorks := 0
		for _, snapshot := range snapshots {
			if state.markSeen(snapshot.Work.Href) {
				newWorks++
			}
		}
		state.incrementWorksBy(newWorks)
		state.incrementEntriesBy(len(snapshots))
		state.incrementLatestEntriesBy(len(snapshots))
		state.incrementPagesWritten()
		if pageErr := state.pageFailure(r.Request.URL.String()); pageErr != nil {
			if state.markPageFinished(r.Request.URL.String()) {
				log.Printf(
					"list page finished with failure: run_id=%s category=%s metric=%s page=%d url=%s written=%d new_works=%d error=%v",
					state.result.RunID,
					task.Category,
					task.Metric,
					pageNumberFromRequest(r.Request.URL),
					r.Request.URL.String(),
					len(snapshots),
					newWorks,
					pageErr,
				)
			}
			return
		}
		if state.markPageFinished(r.Request.URL.String()) {
			log.Printf(
				"list page succeeded: run_id=%s category=%s metric=%s page=%d url=%s written=%d new_works=%d",
				state.result.RunID,
				task.Category,
				task.Metric,
				pageNumberFromRequest(r.Request.URL),
				r.Request.URL.String(),
				len(snapshots),
				newWorks,
			)
		}
	})

	c.OnHTML(metacritic.SelectorPagination, func(e *colly.HTMLElement) {
		state.mu.Lock()
		if state.pagesScheduled {
			state.mu.Unlock()
			return
		}

		maxFoundPage := metacritic.ParsePagination(e)
		finalMaxPage := maxFoundPage
		if task.MaxPages > 0 && task.MaxPages < finalMaxPage {
			finalMaxPage = task.MaxPages
		}

		state.pagesScheduled = true
		state.setPagesScheduledLocked(finalMaxPage)
		state.mu.Unlock()

		for page := 2; page <= finalMaxPage; page++ {
			pageURL := metacritic.BuildListURLWithBase(s.cfg.BaseURL, task.Category, task.Metric, task.Filter, page)
			failuresBeforeVisit := state.failureCount()
			if err := c.Visit(pageURL); err != nil {
				if state.failureCount() == failuresBeforeVisit {
					state.recordFailure(fmt.Errorf("visit page failed: %s: %w", pageURL, err))
				}
				if s.cfg.Debug {
					log.Printf("visit page failed: %s err=%v", pageURL, err)
				}
			}
		}
	})
}

func (s *listRunState) nextRank(page int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageRanks[page]++
	return s.pageRanks[page]
}

func (s *listRunState) incrementWorksBy(count int) {
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.WorksUpserted += count
}

func (s *listRunState) incrementEntriesBy(count int) {
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.ListEntriesInserted += count
}

func (s *listRunState) incrementLatestEntriesBy(count int) {
	if count <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.LatestEntriesUpserted += count
}

func (s *listRunState) recordFailure(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.Failures++
	if s.firstErr == nil {
		if err == nil {
			err = fmt.Errorf("list crawl failed")
		}
		s.firstErr = err
	}
}

func (s *listRunState) markSeen(href string) bool {
	if href == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seenWorks[href] {
		return false
	}
	s.seenWorks[href] = true
	return true
}

func (s *listRunState) snapshotResult() ListRunResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (s *listRunState) setPagesScheduled(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setPagesScheduledLocked(count)
}

func (s *listRunState) setPagesScheduledLocked(count int) {
	if count > s.result.PagesScheduled {
		s.result.PagesScheduled = count
	}
}

func (s *listRunState) incrementPagesSucceeded() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.PagesSucceeded++
	s.result.PagesVisited = s.result.PagesSucceeded
}

func (s *listRunState) incrementPagesWritten() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result.PagesWritten++
}

func (s *listRunState) appendPageSnapshot(url string, snapshot storage.ListEntrySnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageSnapshots[url] = append(s.pageSnapshots[url], snapshot)
}

func (s *listRunState) takePageSnapshots(url string) []storage.ListEntrySnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshots := s.pageSnapshots[url]
	delete(s.pageSnapshots, url)
	return snapshots
}

func (s *listRunState) firstError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

func (s *listRunState) markPageStarted(url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pageStarted[url] {
		return false
	}
	s.pageStarted[url] = true
	return true
}

func (s *listRunState) markPageFinished(url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pageFinished[url] {
		return false
	}
	s.pageFinished[url] = true
	return true
}

func (s *listRunState) setPageFailure(url string, err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pageFailures[url]; ok {
		return false
	}
	s.pageFailures[url] = err
	return true
}

func (s *listRunState) pageFailure(url string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pageFailures[url]
}

func (s *listRunState) failureCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result.Failures
}

func allowedDomainsForBaseURL(raw string) ([]string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("parse base url: %w", err)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("base url must include a hostname")
	}
	return []string{u.Hostname()}, nil
}

func pageNumberFromRequest(u *url.URL) int {
	if u == nil {
		return 1
	}

	raw := strings.TrimSpace(u.Query().Get("page"))
	if raw == "" {
		return 1
	}

	page, err := strconv.Atoi(raw)
	if err != nil || page <= 0 {
		return 1
	}
	return page
}
