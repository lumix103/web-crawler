package crawler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
	Link    string `json:"link"`
	Retries uint8  `json:"retries"`
	Depth   uint8  `json:"depth"`
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
	db         *mongo.Database
	collection *mongo.Collection
}

func NewURLMetadataManager(db *mongo.Database) *URLMetadataManager {
	collection := db.Collection("pages")
	
	// Create unique index on link field
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "link", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	
	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		panic(fmt.Sprintf("Failed to create link index: %v", err))
	}
	
	return &URLMetadataManager{
		db:         db,
		collection: collection,
	}
}

func (m *URLMetadataManager) Set(url string, metadata URLMetaData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := m.collection.ReplaceOne(ctx, bson.M{"link": url},
		metadata,
		options.Replace().SetUpsert(true))

	return err
}

func (m *URLMetadataManager) Get(url string) (URLMetaData, bool) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    var metadata URLMetaData
    err := m.collection.FindOne(ctx, bson.M{"link": url}).Decode(&metadata)
    
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
	db         *mongo.Database
	collection *mongo.Collection
}

func NewDomainMetadataManager(db *mongo.Database) *DomainMetadataManager {
	collection := db.Collection("domains")
	
	// Create unique index on domain field
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "domain", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	
	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		panic(fmt.Sprintf("Failed to create domain index: %v", err))
	}
	
	return &DomainMetadataManager{
		db:         db,
		collection: collection,
	}
}

func (m *DomainMetadataManager) Set(domain string, metadata DomainMetaData) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := m.collection.ReplaceOne(ctx, bson.M{"domain": domain},
		metadata,
		options.Replace().SetUpsert(true))

	return err
}

func (m *DomainMetadataManager) Get(domain string) (DomainMetaData, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    var metadata DomainMetaData
    err := m.collection.FindOne(ctx, bson.M{"domain": domain}).Decode(&metadata)
    
    if err != nil {
        return DomainMetaData{}, false
    }
    
    return metadata, true
}

// CrawlQueue safely manages the job queue.
type CrawlQueue struct {
	sqsClient *sqs.Client
	queueURL  string
	ctx       context.Context
}

// NewCrawlQueue creates a new SQS-backed crawl queue
func NewCrawlQueue(queueURL string) (*CrawlQueue, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &CrawlQueue{
		sqsClient: sqs.NewFromConfig(cfg),
		queueURL:  queueURL,
		ctx:       context.Background(),
	}, nil
}

// NewCrawlQueueWithConfig creates a new queue with custom AWS config
func NewCrawlQueueWithConfig(queueURL string, cfg aws.Config) *CrawlQueue {
	return &CrawlQueue{
		sqsClient: sqs.NewFromConfig(cfg),
		queueURL:  queueURL,
		ctx:       context.Background(),
	}
}

// Add sends a job to the SQS queue
func (q *CrawlQueue) Add(job CrawlJob) error {
	jobData, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	_, err = q.sqsClient.SendMessage(q.ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(q.queueURL),
		MessageBody: aws.String(string(jobData)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Retries": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.Itoa(int(job.Retries))),
			},
			"Depth": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.Itoa(int(job.Depth))),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to send message to SQS: %w", err)
	}

	return nil
}

// Next receives and processes the next job from the SQS queue
func (q *CrawlQueue) Next() (CrawlJob, bool) {
	result, err := q.sqsClient.ReceiveMessage(q.ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1, // Short polling - adjust as needed
		MessageAttributeNames: []string{"All"},
	})

	if err != nil || len(result.Messages) == 0 {
		return CrawlJob{}, false
	}

	message := result.Messages[0]
	var job CrawlJob

	if err := json.Unmarshal([]byte(*message.Body), &job); err != nil {
		// Log error and delete malformed message
		q.deleteMessage(*message.ReceiptHandle)
		return CrawlJob{}, false
	}

	// Delete the message from the queue after successful processing
	if err := q.deleteMessage(*message.ReceiptHandle); err != nil {
		// Log error but continue - message will become visible again
		// after visibility timeout
	}

	return job, true
}

// deleteMessage removes a processed message from the queue
func (q *CrawlQueue) deleteMessage(receiptHandle string) error {
	_, err := q.sqsClient.DeleteMessage(q.ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

// AddBatch adds multiple jobs efficiently using SQS batch operations
func (q *CrawlQueue) AddBatch(jobs []CrawlJob) error {
	if len(jobs) == 0 {
		return nil
	}

	// SQS batch limit is 10 messages
	const batchSize = 10
	
	for i := 0; i < len(jobs); i += batchSize {
		end := i + batchSize
		if end > len(jobs) {
			end = len(jobs)
		}

		batch := jobs[i:end]
		if err := q.sendBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (q *CrawlQueue) sendBatch(jobs []CrawlJob) error {
	entries := make([]types.SendMessageBatchRequestEntry, len(jobs))
	
	for i, job := range jobs {
		jobData, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job %d: %w", i, err)
		}

		entries[i] = types.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("job_%d", i)),
			MessageBody: aws.String(string(jobData)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"Retries": {
					DataType:    aws.String("Number"),
					StringValue: aws.String(strconv.Itoa(int(job.Retries))),
				},
				"Depth": {
					DataType:    aws.String("Number"),
					StringValue: aws.String(strconv.Itoa(int(job.Depth))),
				},
			},
		}
	}

	_, err := q.sqsClient.SendMessageBatch(q.ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(q.queueURL),
		Entries:  entries,
	})

	return err
}