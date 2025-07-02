package main

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/lumix103/web-crawler/internal/crawler"
	"github.com/lumix103/web-crawler/internal/robots"
)

func main() {
	file, err := os.Create("crawl_log.txt")
	if err != nil {
		fmt.Println("Error creating log file:", err)
		return
	}
	defer file.Close()

	urls := crawler.NewURLMetadataManager()
	domains := crawler.NewDomainMetadataManager()
	queue := crawler.NewCrawlQueue()

	c := crawler.NewCrawler("fabs_bot/1.0", urls, domains, queue)
	c.AddJob("https://en.wikipedia.org/wiki/Web_crawler")

	for range 6 {
		job, ok := c.GetJob()
		if !ok {
			break
		}

		domain, err := url.Parse(job.Link)
		if err != nil {
			continue
		}

		domainMetadata, err := c.ProcessRobotFile(domain.Scheme + "://" + domain.Host + "/robots.txt")
		if err != nil {
			continue
		}

		if !domainMetadata.IsPathAllowed(domain.Path, robots.MatchesPatterns) {
            fmt.Fprintf(file, "Request to %s is not allowed\n", job.Link)
            continue
        }

		time.Sleep(domainMetadata.CrawlDelay)

		body, err := c.ProcessJob(job)
		if err != nil {
			fmt.Printf("Error occured: %s\n", err)
			return
		}

		links := c.ExtractLinks(body, job.Link)

		fmt.Fprintf(file, "Crawled: %s\n", job.Link)
		for _, link := range links {
			fmt.Fprintf(file, "\tfound: %s\n", link)
			c.AddJob(link)
		}
	}
}