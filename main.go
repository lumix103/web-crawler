package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
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

type WebCrawler struct {
	Client    *http.Client
	userAgent string
	Job       *CrawlJob
}

const MAX_RETRIES = 10

var URL_METADATA_MAP = make(map[string]URLMetaData)
var DOMAIN_METADATA_MAP = make(map[string]DomainMetaData)
var CRAWL_DEQUEUE = make([]CrawlJob, 0)

func MakeWebCrawler(userAgent string) *WebCrawler {
	wc := WebCrawler{nil, userAgent, nil}
	wc.Client = &http.Client{
		Timeout: 1 * time.Second,
	}
	return &wc
}

// Quick workaround for now
func matchesPatterns(path string, rule string) bool {
	if rule == "" {
		return false
	}

	if strings.HasSuffix(rule, "*") {
		prefix := strings.TrimSuffix(rule, "*")
		return strings.HasPrefix(path, prefix)
	}

	return strings.HasPrefix(path, rule)
}

func (dm *DomainMetaData) IsPathAllowed(link string) bool {
	for _, rule := range dm.Allow {
		if matchesPatterns(link, rule) {
			return true
		}
	}
	for _, rule := range dm.Disallow {
		if matchesPatterns(link, rule) {
			return false
		}
	}
	return true
}

func parseRobotsTxt(body io.Reader, userAgent string) ([]string, []string, time.Duration) {
	scanner := bufio.NewScanner(body)

	var disallowRules []string
	var allowRules []string
	var crawlDelay time.Duration

	var allUserAgentRules = make(map[string][]string)
	var allCrawlDelays = make(map[string]time.Duration)
	var currentUserAgents []string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(strings.ToLower(parts[0]))
		value := strings.TrimSpace(parts[1])

		switch key {
		case "user-agent":
			// Start of a new agent group
			currentUserAgents = []string{value}
		case "disallow":
			for _, agent := range currentUserAgents {
				allUserAgentRules[agent] = append(allUserAgentRules[agent], value)
			}
		case "allow":
			for _, agent := range currentUserAgents {
				allUserAgentRules[agent] = append(allUserAgentRules[agent], value)
			}
		case "crawl-delay":
			delay, err := strconv.Atoi(value)
			if err == nil {
				for _, agent := range currentUserAgents {
					allCrawlDelays[agent] = time.Duration(delay) * time.Second
				}
			}
		}
	}

	// Prioritize specific user agent
	if rules, ok := allUserAgentRules[strings.ToLower(userAgent)]; ok {
		disallowRules = rules
		if delay, ok := allCrawlDelays[strings.ToLower(userAgent)]; ok {
			crawlDelay = delay
		}
		return disallowRules, allowRules, crawlDelay
	}

	// Fallback to wildcard
	if rules, ok := allUserAgentRules["*"]; ok {
		disallowRules = rules
		if delay, ok := allCrawlDelays["*"]; ok {
			crawlDelay = delay
		}
		return disallowRules, allowRules, crawlDelay
	}

	return disallowRules, allowRules, crawlDelay // Return empty rules and zero delay if no match
}

func (wc *WebCrawler) ProcessRobotFile(link string) (DomainMetaData, error) {
	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return DomainMetaData{}, err
	}

	req.Header.Set("User-Agent", wc.userAgent)
	resp, err := wc.Client.Do(req)
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

	disallowRules, allowRules, crawlDelay := parseRobotsTxt(resp.Body, wc.userAgent)

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

func (wc *WebCrawler) ProcessJob(cj CrawlJob) ([]byte, error) {
	req, err := http.NewRequest("GET", cj.Link, nil)
	if err != nil {
		cj.Retries += 1
		if cj.Retries < MAX_RETRIES {
			CRAWL_DEQUEUE = append(CRAWL_DEQUEUE, cj)
		}
		return nil, err
	}

	req.Header.Set("User-Agent", wc.userAgent)
	resp, err := wc.Client.Do(req)
	if err != nil {
		fmt.Printf("Error occured: %s\n", err)
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (wc *WebCrawler) ExtractLinks(body []byte, baseURL string) []string {
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

func main() {
	file, err := os.Create("crawl_log.txt")
	if err != nil {
		fmt.Println("Error creating log file:", err)
		return
	}
	defer file.Close()

	CRAWL_DEQUEUE = append(CRAWL_DEQUEUE, CrawlJob{"https://en.wikipedia.org/wiki/Web_crawler", 0})
	wc := MakeWebCrawler("fabs_bot/1.0")
	for i := 0; i < 6 && len(CRAWL_DEQUEUE) > 0; i++ {
		job := CRAWL_DEQUEUE[0]
		CRAWL_DEQUEUE = CRAWL_DEQUEUE[1:]

		domain, err := url.Parse(job.Link)
		if err != nil {
			continue
		}

		if _, ok := DOMAIN_METADATA_MAP[domain.Host]; !ok {
			domain_metadata, err := wc.ProcessRobotFile(domain.Scheme + "://" + domain.Host + "/robots.txt")
			if err != nil {
				continue
			}
			domain_metadata.LastCrawlTime = time.Now().Add(-domain_metadata.CrawlDelay)
			DOMAIN_METADATA_MAP[domain.Host] = domain_metadata
		}

		domain_metadata := DOMAIN_METADATA_MAP[domain.Host]
        if !domain_metadata.LastCrawlTime.IsZero() && domain_metadata.CrawlDelay > 0 {
            timeSinceLastCrawl := time.Since(domain_metadata.LastCrawlTime)
            if timeSinceLastCrawl < domain_metadata.CrawlDelay {
                waitTime := domain_metadata.CrawlDelay - timeSinceLastCrawl
                fmt.Fprintf(file, "Waiting for %s due to crawl-delay for domain %s\n", waitTime, domain.Host)
                time.Sleep(waitTime)
            }
        }

		if !domain_metadata.IsPathAllowed(domain.Path) {
			fmt.Fprintf(file, "Request to %s is not allowed\n", job.Link)
			continue
		}

		if _, ok := URL_METADATA_MAP[job.Link]; !ok {
			URL_METADATA_MAP[job.Link] = URLMetaData{
				Link:          job.Link,
				LastCrawlTime: time.Now(),
				Depth:         1,
			}
		} else {
			continue
		}

		body, err := wc.ProcessJob(job)
		if err != nil {
			fmt.Printf("Error occured: %s\n", err)
			return
		}

		domain_metadata.LastCrawlTime = time.Now()
		DOMAIN_METADATA_MAP[domain.Host] = domain_metadata

		links := wc.ExtractLinks(body, job.Link)

		fmt.Fprintf(file, "Crawled: %s\n", job.Link)
		for _, link := range links {
			fmt.Fprintf(file, "\tfound: %s\n", link)
			CRAWL_DEQUEUE = append(CRAWL_DEQUEUE, CrawlJob{link, 0})
		}

	}
}

