package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/tls"

	"bytes"
	"io/ioutil"

	"github.com/PuerkitoBio/goquery"
	"github.com/joho/godotenv"
)

// SearchResult defines the structure for a single search result
type SearchResult struct {
	Title          string `json:"title"`
	URL            string `json:"url"`
	Snippet        string `json:"snippet"`
	Date           string `json:"date"`
	ArticleContent string `json:"article_content"`
}

// Response structure to include both the refined HTML and structured JSON
type SearchResponse struct {
	Results []SearchResult `json:"results"`
}

// GoogleAPIResponse represents the structure of the Google Custom Search API response
type GoogleAPIResponse struct {
	Items []struct {
		Title   string `json:"title"`
		Link    string `json:"link"`
		Snippet string `json:"snippet"`
	} `json:"items"`
}

// GenerateSearchQuery uses OpenAI to turn a user prompt into a focused web search query
func GenerateSearchQuery(prompt string) (string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("OPENAI_API_KEY not set")
	}

	currentYear := time.Now().Year()
	currentYearString := strconv.Itoa(currentYear)

	reqBody := map[string]interface{}{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "system", "content": "You are a helpful assistant that optimizes user questions into concise, effective web search queries in order to get the most relevant results to answer the user question. Current year is " + currentYearString + ". Only output the search query, nothing else."},
			{"role": "user", "content": prompt},
		},
		"max_tokens":  32,
		"temperature": 0.2,
	}
	jsonData, _ := json.Marshal(reqBody)

	req, _ := http.NewRequest("POST", "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", fmt.Errorf("OpenAI API error: %s, %s", resp.Status, string(body))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices returned from OpenAI")
	}
	return strings.TrimSpace(result.Choices[0].Message.Content), nil
}

// DecomposePrompt uses OpenAI to break down a user prompt into a list of focused web search queries
func DecomposePrompt(prompt string) ([]string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("OPENAI_API_KEY not set")
	}

	currentYear := time.Now().Year()
	currentYearString := strconv.Itoa(currentYear)

	systemPrompt := "You are a helpful assistant that breaks down complex user questions into a list of focused web search queries for all relevant parts of the user question. Only output a JSON array of search queries. Current year is " + currentYearString + "."

	reqBody := map[string]interface{}{
		"model": "gpt-4o",
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": prompt},
		},
		"max_tokens":  128,
		"temperature": 0.2,
	}
	jsonData, _ := json.Marshal(reqBody)

	req, _ := http.NewRequest("POST", "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(jsonData))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("OpenAI API error: %s, %s", resp.Status, string(body))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Choices) == 0 {
		return nil, fmt.Errorf("no choices returned from OpenAI")
	}

	content := strings.TrimSpace(result.Choices[0].Message.Content)
	// Try to parse as JSON array
	var queries []string
	if err := json.Unmarshal([]byte(content), &queries); err != nil {
		// fallback: treat as single query
		return []string{content}, nil
	}
	return queries, nil
}

// Search no longer depends on http.Request or http.ResponseWriter.
// Instead, you can call it directly with a query string.
func Search(prompt string) (SearchResponse, error) {
	// Load environment variables (e.g., GOOGLE_API_KEY, GOOGLE_CSE_ID)
	// Adjust the path to .env if needed
	if err := godotenv.Load("../../.env"); err != nil {
		log.Println("Warning: could not load ../../.env file:", err)
		// Not fatal, in case your environment variables are already set
	}

	// Validate query is not empty
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		log.Printf("Empty search query received, skipping API call")
		return SearchResponse{
			Results: []SearchResult{
				{
					Title:   "No search query provided",
					URL:     "",
					Snippet: "Please provide a search query to get results.",
					Date:    time.Now().Format("2006-01-02"),
				},
			},
		}, nil
	}

	log.Printf("Received user prompt: %s", prompt)

	// Decompose prompt into search queries
	queries, err := DecomposePrompt(prompt)
	if err != nil || len(queries) == 0 {
		log.Printf("Error decomposing prompt, falling back to single query: %v", err)
		queries = []string{prompt}
	}

	var allResults []SearchResult
	for _, query := range queries {
		log.Printf("Running search for: %s", query)
		searchQuery, err := GenerateSearchQuery(query)
		if err != nil {
			log.Printf("Error generating search query: %v", err)
			searchQuery = query
		}
		log.Printf("Using search query: %s", searchQuery)

		apiKey := os.Getenv("GOOGLE_API_KEY")
		cx := os.Getenv("GOOGLE_CSE_ID")
		if apiKey == "" || cx == "" {
			return SearchResponse{}, fmt.Errorf("GOOGLE_API_KEY or GOOGLE_CSE_ID environment variable missing")
		}

		numResults := 10
		apiURL := "https://customsearch.googleapis.com/customsearch/v1"

		// Build the request URL
		reqURL, err := url.Parse(apiURL)
		if err != nil {
			return SearchResponse{}, fmt.Errorf("error parsing API URL: %v", err)
		}

		queryParams := reqURL.Query()
		queryParams.Set("key", apiKey)
		queryParams.Set("cx", cx)
		queryParams.Set("q", searchQuery)
		queryParams.Set("num", fmt.Sprintf("%d", numResults))
		reqURL.RawQuery = queryParams.Encode()

		// Make the API request
		resp, err := http.Get(reqURL.String())
		if err != nil {
			log.Printf("error making API request: %v", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("Google API returned status code %d", resp.StatusCode)
			continue
		}

		var apiResponse GoogleAPIResponse
		if err = json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
			log.Printf("error parsing API response: %v", err)
			continue
		}

		numToScrape := 6
		if len(apiResponse.Items) < numToScrape {
			numToScrape = len(apiResponse.Items)
		}

		type scrapeResult struct {
			idx     int
			content string
		}
		scrapeCh := make(chan scrapeResult, numToScrape)
		var wg sync.WaitGroup

		for i := 0; i < numToScrape; i++ {
			wg.Add(1)
			go func(idx int, link string) {
				defer wg.Done()
				scrapeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				contentCh := make(chan string, 1)
				go func() {
					content, err := scrapeArticleContent(link)
					if err != nil {
						log.Printf("Warning: Could not scrape content from %s: %v", link, err)
						contentCh <- ""
					} else {
						contentCh <- content
					}
				}()
				select {
				case content := <-contentCh:
					scrapeCh <- scrapeResult{idx: idx, content: content}
				case <-scrapeCtx.Done():
					log.Printf("Scrape timeout for %s", link)
					scrapeCh <- scrapeResult{idx: idx, content: ""}
				}
			}(i, apiResponse.Items[i].Link)
		}

		articleContents := make([]string, numToScrape)
		go func() {
			wg.Wait()
			close(scrapeCh)
		}()
		for res := range scrapeCh {
			articleContents[res.idx] = res.content
			if len(articleContents) == numToScrape {
				break
			}
		}

		for i, item := range apiResponse.Items {
			var articleContent string
			if i < numToScrape {
				articleContent = articleContents[i]
			}
			allResults = append(allResults, SearchResult{
				Title:          item.Title,
				URL:            item.Link,
				Snippet:        item.Snippet,
				Date:           "",
				ArticleContent: articleContent,
			})
		}
	}

	log.Printf("Total results found: %d", len(allResults))
	return SearchResponse{Results: allResults}, nil
}

// scrapeArticleContent fetches and extracts the main content from a webpage
func scrapeArticleContent(url string) (string, error) {
	// Create a client with timeout and proper headers
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// Create request with proper headers
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	// Add headers to mimic a real browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Cache-Control", "max-age=0")

	// Make the request
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error fetching URL: %v", err)
	}
	defer resp.Body.Close()

	// Check if the response is successful
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error response status: %d", resp.StatusCode)
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error parsing HTML: %v", err)
	}

	// Remove unwanted elements
	doc.Find("script, style, nav, footer, header, iframe, noscript, .ad, .advertisement, .banner, .popup, .modal, .cookie-banner").Remove()

	// Extract text content
	var content strings.Builder

	// Always extract all visible text from the <body> after removing unwanted elements
	doc.Find("body").Each(func(i int, s *goquery.Selection) {
		content.WriteString(s.Text())
		content.WriteString("\n")
	})

	// Clean up the content
	cleanedContent := strings.TrimSpace(content.String())
	cleanedContent = strings.Join(strings.Fields(cleanedContent), " ") // Normalize whitespace

	return cleanedContent, nil
}
