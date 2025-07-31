# Go Web Crawler

A simple web crawler prototype built in Go that fetches web pages and processes them through a job queue system.

## Features

- Job-based crawling with retry mechanism
- Configurable user agent
- HTTP timeout handling
- Queue-based processing for scalability
- Structured metadata tracking for URLs and domains

## Current Status

This is an early prototype focused on establishing the core crawling loop. The crawler currently:

- Fetches HTML content from web pages
- Processes jobs from an in-memory queue
- Implements basic retry logic (up to 10 retries)
- Handles HTTP timeouts and errors

## Usage

```bash
go run main.go
```

The crawler starts with Wikipedia as a seed URL and outputs the fetched HTML content.

## Architecture

### Core Types

- **CrawlJob**: Represents a URL to crawl with retry count
- **WebCrawler**: HTTP client wrapper with user agent configuration
- **URLMetaData**: Tracks crawl history and depth for URLs
- **DomainMetaData**: Stores domain-specific crawling rules and timing

### Job Processing

1. Jobs are stored in a simple slice-based queue (`CRAWL_DEQUEUE`)
2. The main loop processes jobs sequentially
3. Failed requests are retried up to `MAX_RETRIES` times
4. HTTP responses are read completely before moving to the next job

## Next Steps

- [x] HTML parsing and link extraction
- [x] Duplicate URL detection
- [x] Robots.txt compliance
- [x] Domain-specific rate limiting
- [x] Concurrent processing with goroutines
- [x] External queue system integration (SQS/Kafka)

## Configuration

- **Timeout**: 1 second HTTP timeout
- **Max Retries**: 10 retry attempts per URL
- **User Agent**: `fabs_bot/2.0`

## Requirements

- Go 1.21.0 or higher

---
