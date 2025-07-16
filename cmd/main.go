package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/lumix103/web-crawler/internal/crawler"
	"github.com/lumix103/web-crawler/internal/robots"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/joho/godotenv"
)

const (
	NumWorkers = 3
	MaxJobs    = 10
)

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()


    client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGODB_URI")))
    if err != nil {
        log.Fatal(err)
    }
    defer client.Disconnect(ctx)

	db := client.Database("web-crawler")
	
	urls := crawler.NewURLMetadataManager(*db)
	domains := crawler.NewDomainMetadataManager()
	queue := crawler.NewCrawlQueue()

	crawlers := make([]*crawler.Crawler, NumWorkers+1)
	for i := 0; i < NumWorkers+1; i++ {
		crawlers[i] = crawler.NewCrawler("fabs_bot/2.0", urls, domains, queue)
	}

	queue.Add(crawler.CrawlJob{Link: "https://en.wikipedia.org/wiki/Web_crawler", Retries: 0, Depth: 0})

	resultChan := make(chan CrawlResult, MaxJobs)
	doneChan := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < NumWorkers; i++ {
		wg.Add(1)
		go worker(i, resultChan, crawlers[i], &wg)
	}

	go resultProcessor(resultChan, crawlers[NumWorkers], doneChan)

	wg.Wait()
	close(resultChan)
	<-doneChan

	fmt.Println("Crawling completed")
}

type CrawlResult struct {
	Job    crawler.CrawlJob
	Links  []string
	Error  error
	Status string
}

func worker(id int, resultChan chan<- CrawlResult, c *crawler.Crawler, wg *sync.WaitGroup) {
	defer wg.Done()
	
	fmt.Printf("Worker %d started\n", id)
	
	jobsProcessed := 0
	for jobsProcessed < MaxJobs/NumWorkers {
		job, ok := c.GetJob()
		if !ok {
			// No jobs available, wait a bit
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		result := processJob(job, c)
		resultChan <- result
		jobsProcessed++
	}
	
	fmt.Printf("Worker %d finished\n", id)
}

func processJob(job crawler.CrawlJob, c *crawler.Crawler) CrawlResult {
	result := CrawlResult{
		Job:   job,
		Links: []string{},
	}

	// Parse URL
	domain, err := url.Parse(job.Link)
	if err != nil {
		result.Error = err
		result.Status = "parse_error"
		return result
	}

	// Check robots.txt
	domainMetadata, err := c.ProcessRobotFile(domain.Scheme + "://" + domain.Host + "/robots.txt")
	if err != nil {
		result.Error = err
		result.Status = "robots_error"
		return result
	}

	if !domainMetadata.IsPathAllowed(domain.Path, robots.MatchesPatterns) {
		result.Status = "not_allowed"
		return result
	}

	// Respect crawl delay
	c.EnforceCrawlDelay(domain.Host, domainMetadata.CrawlDelay)
	// Process the job
	body, err := c.ProcessJob(job)
	if err != nil {
		result.Error = err
		result.Status = "process_error"
		return result
	}

	// Extract links
	links := c.ExtractLinks(body, job.Link)
	result.Links = links
	result.Status = "success"

	return result
}

func resultProcessor(resultChan <-chan CrawlResult, c *crawler.Crawler, doneChan chan<- struct{}) {
	for result := range resultChan {
		switch result.Status {
		case "success":
            //fmt.Fprintf(file, "Crawled: %s (Depth: %d)\n", result.Job.Link, result.Job.Depth)
            c.MarkAsCrawled(result.Job.Link, result.Job.Depth)
			newDepth := result.Job.Depth + 1
            for _, link := range result.Links {
                //fmt.Fprintf(file, "\tfound: %s\n", link)
                // Add new jobs to the crawler's queue
                if newDepth < 255 {
                    c.AddJobIfNotVisited(link, 0, newDepth)
                }
            }
        case "not_allowed":

			//fmt.Fprintf(file, "Request to %s is not allowed\n", result.Job.Link)
		case "parse_error", "robots_error", "process_error":
			//fmt.Fprintf(file, "Error processing %s: %v\n", result.Job.Link, result.Error)
			if (result.Job.Retries + 1 < crawler.MAX_RETRIES) {
				c.AddJob(result.Job.Link, result.Job.Retries + 1, result.Job.Depth)
			}
		}
	}
	doneChan <- struct{}{}
}