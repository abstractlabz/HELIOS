package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"crypto/tls"

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

// Search no longer depends on http.Request or http.ResponseWriter.
// Instead, you can call it directly with a query string.
func Search(query string) (SearchResponse, error) {
	// Load environment variables (e.g., GOOGLE_API_KEY, GOOGLE_CSE_ID)
	// Adjust the path to .env if needed
	if err := godotenv.Load("../../.env"); err != nil {
		log.Println("Warning: could not load ../../.env file:", err)
		// Not fatal, in case your environment variables are already set
	}

	log.Printf("Received search query: %s", query)

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
	queryParams.Set("q", query)
	queryParams.Set("num", fmt.Sprintf("%d", numResults))
	reqURL.RawQuery = queryParams.Encode()

	// Make the API request
	resp, err := http.Get(reqURL.String())
	if err != nil {
		return SearchResponse{}, fmt.Errorf("error making API request: %v", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		return SearchResponse{}, fmt.Errorf("Google API returned status code %d", resp.StatusCode)
	}

	// Parse the API response
	var apiResponse GoogleAPIResponse
	if err = json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return SearchResponse{}, fmt.Errorf("error parsing API response: %v", err)
	}

	// Extract search results
	var searchResults []SearchResult
	for i, item := range apiResponse.Items {
		// Only scrape content for the first 3 articles
		var articleContent string
		if i < 3 {
			content, err := scrapeArticleContent(item.Link)
			if err != nil {
				log.Printf("Warning: Could not scrape content from %s: %v", item.Link, err)
				articleContent = "" // Set empty string if scraping fails
			} else {
				articleContent = content
			}
		}

		searchResults = append(searchResults, SearchResult{
			Title:          item.Title,
			URL:            item.Link,
			Snippet:        item.Snippet,
			Date:           "", // Google doesn't provide date
			ArticleContent: articleContent,
		})
	}

	log.Printf("Total results found: %d", len(searchResults))

	// Return a structured SearchResponse
	return SearchResponse{Results: searchResults}, nil
}

// scrapeArticleContent fetches and extracts the main content from a webpage
func scrapeArticleContent(url string) (string, error) {
	// Create a client with timeout and proper headers
	client := &http.Client{
		Timeout: 10 * time.Second,
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

	// Try to find the main content area with more specific selectors
	mainContent := doc.Find("article, main, .content, .article, .post, #content, .main, .story-body, .article-body, .post-content, .entry-content")
	if mainContent.Length() > 0 {
		mainContent.Each(func(i int, s *goquery.Selection) {
			content.WriteString(s.Text())
			content.WriteString("\n")
		})
	} else {
		// Fallback to body text if no main content area is found
		doc.Find("body").Each(func(i int, s *goquery.Selection) {
			content.WriteString(s.Text())
			content.WriteString("\n")
		})
	}

	// Clean up the content
	cleanedContent := strings.TrimSpace(content.String())
	cleanedContent = strings.Join(strings.Fields(cleanedContent), " ") // Normalize whitespace

	return cleanedContent, nil
}
