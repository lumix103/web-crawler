package robots

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"time"
)

// Quick workaround for now
func MatchesPatterns(path string, rule string) bool {
	if rule == "" {
		return false
	}

	if strings.HasSuffix(rule, "*") {
		prefix := strings.TrimSuffix(rule, "*")
		return strings.HasPrefix(path, prefix)
	}

	return strings.HasPrefix(path, rule)
}

func ParseRobotsTxt(body io.Reader, userAgent string) ([]string, []string, time.Duration) {
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
