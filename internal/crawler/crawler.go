package crawler

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/html"

	"github.com/lumix103/web-crawler/internal/robots"
)

const MAX_RETRIES = 10

type Crawler struct {
	client    *http.Client
	userAgent string
	urls      *URLMetadataManager
	domains   *DomainMetadataManager
	queue     *CrawlQueue
}

func NewCrawler(userAgent string, urls *URLMetadataManager, domains *DomainMetadataManager, queue *CrawlQueue) *Crawler {
	return &Crawler{
		client: &http.Client{
			Timeout: 1 * time.Second,
		},
		userAgent: userAgent,
		urls:      urls,
		domains:   domains,
		queue:     queue,
	}
}

func (c *Crawler) AddJob(link string, retries uint8, depth uint8) {
	c.queue.Add(CrawlJob{Link: link, Retries: retries, Depth: depth})
}

func (c *Crawler) GetJob() (CrawlJob, bool) {
	return c.queue.Next()
}

func (c *Crawler) MarkAsCrawled(link string, depth uint8) {
	c.urls.Set(link, URLMetaData{Link: link, LastCrawlTime: time.Now(), Depth: depth})
}

func (c *Crawler) AddJobIfNotVisited(link string, retries uint8, depth uint8) {
	if c.urls.TestAndSet(link, URLMetaData{Link: link, LastCrawlTime: time.Now(), Depth: depth}) {
		c.AddJob(link, retries, depth)
	}
}

func (c *Crawler) ProcessRobotFile(link string) (DomainMetaData, error) {
	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return DomainMetaData{}, err
	}

	req.Header.Set("User-Agent", c.userAgent)
	resp, err := c.client.Do(req)
	if err != nil {
		return DomainMetaData{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return DomainMetaData{
			Disallow:   []string{},
			Allow:      []string{},
			CrawlDelay: 1 * time.Second, // Default politeness
		}, nil
	}

	if resp.StatusCode != http.StatusOK {
		return DomainMetaData{}, fmt.Errorf("failed to fetch robots.txt: %s", resp.Status)
	}

	disallowRules, allowRules, crawlDelay := robots.ParseRobotsTxt(resp.Body, c.userAgent)

	if crawlDelay == 0 {
		crawlDelay = 1 * time.Second // Default politeness
	}

	u, err := url.Parse(link)
	if err != nil {
		return DomainMetaData{}, err
	}

	return DomainMetaData{
		Domain:     u.Host,
		Disallow:   disallowRules,
		Allow:      allowRules,
		CrawlDelay: crawlDelay,
	}, nil
}
// Fix this later but if this fails then we should not crawl at all
func (c *Crawler) EnforceCrawlDelay(domain string, crawlDelay time.Duration) {
    metadata, ok := c.domains.Get(domain)
    if !ok {
        // If no metadata, assume 10 second delay (or fetch it here if needed)
		time.Sleep(time.Second * 10)
        return
    }

    now := time.Now()
    nextAllowed := metadata.LastCrawlTime.Add(crawlDelay) // Use the stored delay
    if now.Before(nextAllowed) {
        time.Sleep(nextAllowed.Sub(now))
    }

    // Update last crawl time optimistically (refine to after successful crawl if possible)
    metadata.LastCrawlTime = time.Now()
	c.domains.Set(domain, metadata)
}

func (c *Crawler) ProcessJob(cj CrawlJob) ([]byte, error) {
	req, err := http.NewRequest("GET", cj.Link, nil)
	if err != nil {
		cj.Retries += 1
		if cj.Retries < MAX_RETRIES {
			c.queue.Add(cj)
		}
		return nil, err
	}

	req.Header.Set("User-Agent", c.userAgent)
	resp, err := c.client.Do(req)
	if err != nil {
		fmt.Printf("Error occured: %s", err)
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *Crawler) ExtractLinks(body []byte, baseURL string) []string {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil
	}

	base, err := url.Parse(baseURL)
	if err != nil {
		return nil
	}

	links := make([]string, 0)
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" {
					link, err := url.Parse(a.Val)
					if err != nil {
						continue
					}
					absolute := base.ResolveReference(link)
					if absolute.Scheme != "http" && absolute.Scheme != "https" {
						continue
					}

					next := url.URL{
						Scheme: absolute.Scheme,
						Host:   absolute.Host,

						Path: absolute.Path,
					}

					links = append(links, next.String())
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return links
}