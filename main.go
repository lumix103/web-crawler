package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
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

func (wc *WebCrawler) ExtractLinks(body []byte) []string {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	links := make([]string, 0)
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" {
					links = append(links, a.Val)
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
	CRAWL_DEQUEUE = append(CRAWL_DEQUEUE, CrawlJob{"https://www.wikipedia.org/", 0})
	wc := MakeWebCrawler("fabs_bot/1.0")
	for len(CRAWL_DEQUEUE) > 0 {
		job := CRAWL_DEQUEUE[0]
		CRAWL_DEQUEUE = CRAWL_DEQUEUE[1:]
		body, err := wc.ProcessJob(job)
		if err != nil {
			fmt.Printf("Error occured: %s\n", err)
			return
		}
		fmt.Println(wc.ExtractLinks(body))
	}
}
