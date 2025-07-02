package crawler

import (
	"sync"
	"time"
)

type URLMetaData struct {
	Link          string
	LastCrawlTime time.Time
	Depth         uint8
}

// TODO: Consider turning the allow and disallow into a trie
type DomainMetaData struct {
	Domain        string
	LastCrawlTime time.Time
	UserAgent     string
	Disallow      []string
	Allow         []string
	CrawlDelay    time.Duration
}

type CrawlJob struct {
	Link    string
	Retries uint8
}

func (dm *DomainMetaData) IsPathAllowed(link string, robots_pkg func(string, string) bool) bool {
	for _, rule := range dm.Allow {
		if robots_pkg(link, rule) {
			return true
		}
	}
	for _, rule := range dm.Disallow {
		if robots_pkg(link, rule) {
			return false
		}
	}
	return true
}

// URLMetadataManager safely manages URL metadata.
type URLMetadataManager struct {
	mu   sync.Mutex
	Data map[string]URLMetaData
}

func NewURLMetadataManager() *URLMetadataManager {
	return &URLMetadataManager{
		Data: make(map[string]URLMetaData),
	}
}

func (m *URLMetadataManager) Set(url string, metadata URLMetaData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Data[url] = metadata
}

func (m *URLMetadataManager) Get(url string) (URLMetaData, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	metadata, ok := m.Data[url]
	return metadata, ok
}

// DomainMetadataManager safely manages domain metadata.
type DomainMetadataManager struct {
	mu   sync.Mutex
	Data map[string]DomainMetaData
}

func NewDomainMetadataManager() *DomainMetadataManager {
	return &DomainMetadataManager{
		Data: make(map[string]DomainMetaData),
	}
}

func (m *DomainMetadataManager) Set(domain string, metadata DomainMetaData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Data[domain] = metadata
}

func (m *DomainMetadataManager) Get(domain string) (DomainMetaData, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	metadata, ok := m.Data[domain]
	return metadata, ok
}

// CrawlQueue safely manages the job queue.
type CrawlQueue struct {
	mu   sync.Mutex
	jobs []CrawlJob
}

func NewCrawlQueue() *CrawlQueue {
	return &CrawlQueue{
		jobs: make([]CrawlJob, 0),
	}
}

func (q *CrawlQueue) Add(job CrawlJob) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, job)
}

func (q *CrawlQueue) Next() (CrawlJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return CrawlJob{}, false
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	return job, true
}
