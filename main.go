package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

type DomainMetaData struct {
	Domain        string
	LastCrawlTime time.Time
	UserAgent     string
	Disallow      []string
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

func parseRobotsTxt(body io.Reader, userAgent string) ([]string, time.Duration) {
	scanner := bufio.NewScanner(body)

	var disallowRules []string
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
		return disallowRules, crawlDelay
	}

	// Fallback to wildcard
	if rules, ok := allUserAgentRules["*"]; ok {
		disallowRules = rules
		if delay, ok := allCrawlDelays["*"]; ok {
			crawlDelay = delay
		}
		return disallowRules, crawlDelay
	}

	return disallowRules, crawlDelay // Return empty rules and zero delay if no match
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
			CrawlDelay: 1 * time.Second, // Default politeness
		}, nil
	}

	if resp.StatusCode != http.StatusOK {
		return DomainMetaData{}, fmt.Errorf("failed to fetch robots.txt: %s", resp.Status)
	}

	disallowRules, crawlDelay := parseRobotsTxt(resp.Body, wc.userAgent)

	u, err := url.Parse(link)
	if err != nil {
		return DomainMetaData{}, err
	}

	return DomainMetaData{
		Domain:     u.Host,
		Disallow:   disallowRules,
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
	CRAWL_DEQUEUE = append(CRAWL_DEQUEUE, CrawlJob{"https://en.wikipedia.org/wiki/Web_crawler", 0})
	wc := MakeWebCrawler("fabs_bot/1.0")
	for len(CRAWL_DEQUEUE) > 0 {
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
            DOMAIN_METADATA_MAP[domain.Host] = domain_metadata
            fmt.Printf("Successfully processed robots.txt for %s: %+v\n", domain.Host, domain_metadata)
        }

        // body, err := wc.ProcessJob(job)
        // if err != nil {
        //  fmt.Printf("Error occured: %s\n", err)
        //  return
        // }
        // fmt.Println(wc.ExtractLinks(body, job.Link))
	}
}

