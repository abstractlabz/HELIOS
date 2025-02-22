package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/PuerkitoBio/goquery"
	"github.com/segmentio/kafka-go"
)

type NewsRequest struct {
	Topic          string   `json:"topic"`
	SegmentTargets []string `json:"segment_targets"`
}

func ProcessNewsRequests(buffer chan kafka.Message) {
	for msg := range buffer {
		// Parse the incoming Kafka message
		var request NewsRequest
		if err := json.Unmarshal(msg.Value, &request); err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			continue
		}

		// Process each segment target (ticker)
		for _, ticker := range request.SegmentTargets {
			ticker, tag := removePrefixSuffix(ticker)

			// Scrape news for the ticker
			scrapeTickerURL := "https://news.google.com/search?q=" + strings.ToUpper(ticker) + "-news" + tag + "&hl=en-US&gl=US&ceid=US%3Aen"

			news, err := scrapeTextFromDiv(scrapeTickerURL, 5)
			if err != nil {
				log.Printf("Failed to scrape news for ticker %s: %v", ticker, err)
				continue
			}

			// Log successful scrape
			log.Printf("Successfully scraped news for ticker: %s", ticker)
			fmt.Printf("News data: %s\n", news)
		}
	}
}

// scrapeTextFromDiv scrapes the text from Google's top news section
func scrapeTextFromDiv(url string, collectionSize int) (string, error) {
	res, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return "", fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return "", err
	}

	var results []string
	doc.Find("a.JtKRv").Each(func(i int, s *goquery.Selection) {
		if i < collectionSize {
			headline := s.Text()
			link, exists := s.Attr("href")
			if exists {
				fullURL := "https://news.google.com" + strings.TrimPrefix(link, ".")
				results = append(results, fmt.Sprintf("Headline: %s, URL: %s", headline, fullURL))
			}
		}
	})

	return strings.Join(results, "\n"), nil
}

func removePrefixSuffix(ticker string) (string, string) {
	if strings.HasPrefix(ticker, "X:") {
		ticker = strings.TrimPrefix(ticker, "X:")
		ticker = strings.TrimSuffix(ticker, "USD")
		return ticker, "-crypto"
	}
	return ticker, "-stock"
}

func StartNewsProcessor() {
	// Create a buffer channel for Kafka messages
	buffer := make(chan kafka.Message)

	// Start the Kafka consumer
	go utils.ConsumeToBuffer(buffer, "alert_financials", "news-processor-group", "../../.env")

	// Start processing messages
	ProcessNewsRequests(buffer)
}

func main() {
	log.Println("Starting News Processor...")
	StartNewsProcessor()
}
