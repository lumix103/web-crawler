package crawler

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type URLMetaData struct {
	ID            primitive.ObjectID `bson:"_id,omitempty"`
	Link          string             `bson:"link"`
	LastCrawlTime time.Time          `bson:"last_crawl_time"`
	Depth         uint8              `bson:"depth"`
}

type DomainMetaData struct {
	ID            primitive.ObjectID `bson:"_id,omitempty"`
	Domain        string             `bson:"domain"`
	LastCrawlTime time.Time          `bson:"last_crawl_time"`
	UserAgent     string             `bson:"user_agent"`
	Disallow      []string           `bson:"disallow"`
	Allow         []string           `bson:"allow"`
	CrawlDelay    time.Duration      `bson:"crawl_delay"`
}

type CrawlJob struct {
	ID      primitive.ObjectID `bson:"_id,omitempty"`
	Link    string             `bson:"link"`
	Retries uint8              `bson:"retries"`
	Depth   uint8              `bson:"depth"`
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
	mu         sync.Mutex
	db         *mongo.Database
	collection *mongo.Collection
}

func NewURLMetadataManager(db mongo.Database) *URLMetadataManager {
	return &URLMetadataManager{
		db:         &db,
		collection: db.Collection("pages"),
	}
}

func (m *URLMetadataManager) Set(url string, metadata URLMetaData) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := m.collection.ReplaceOne(ctx, bson.M{"url": url},
		metadata,
		options.Replace().SetUpsert(true))

	return err
}

func (m *URLMetadataManager) Get(url string) (URLMetaData, bool) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    var metadata URLMetaData
    err := m.collection.FindOne(ctx, bson.M{"url": url}).Decode(&metadata)
    
    if err != nil {
        return URLMetaData{}, false
    }
    
    return metadata, true
}

func (m *URLMetadataManager) TestAndSet(url string, metadata URLMetaData) bool {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    metadata.Link = url
    metadata.LastCrawlTime = time.Now()
    
    // Insert only if document doesn't exist
    _, err := m.collection.InsertOne(ctx, metadata)
    
    if err != nil {
        // Check if error is due to duplicate key (URL already exists)
        if mongo.IsDuplicateKeyError(err) {
            return false // Already exists
        }
        // Other error (network, timeout, etc.) - treat as "didn't set"
        return false
    }
    
    return true // Was newly inserted
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
