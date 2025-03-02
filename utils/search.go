package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/joho/godotenv"
)

// SearchResult defines the structure for a single search result
type SearchResult struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet"`
	Date    string `json:"date"`
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
	for _, item := range apiResponse.Items {
		searchResults = append(searchResults, SearchResult{
			Title:   item.Title,
			URL:     item.Link,
			Snippet: item.Snippet,
			Date:    "", // Google doesn't provide date
		})
	}

	log.Printf("Total results found: %d", len(searchResults))

	// Return a structured SearchResponse
	return SearchResponse{Results: searchResults}, nil
}
