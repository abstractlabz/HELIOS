package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/PuerkitoBio/goquery"
	"github.com/segmentio/kafka-go"
)

type NewsRequest struct {
	Topic          string   `json:"topic"`
	SegmentTargets []string `json:"segment_targets"`
}

type ProcessorConfig struct {
	Topic          string `json:"topic"`
	Segment        string `json:"segment"`
	DataList       string `json:"data_list"`
	PromptTemplate string `json:"prompt_template"`
}

type DataList []struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

func processNewsItem(job interface{}) interface{} {
	ticker := job.(string)
	ticker, tag := removePrefixSuffix(ticker)
	scrapeTickerURL := "https://news.google.com/search?q=" + strings.ToUpper(ticker) + "-news" + tag + "&hl=en-US&gl=US&ceid=US%3Aen"

	news, err := scrapeTextFromDiv(scrapeTickerURL, 5)
	if err != nil {
		log.Printf("Failed to scrape news for ticker %s: %v", ticker, err)
		return nil
	}

	return map[string]interface{}{
		"ticker": ticker,
		"data":   news,
	}
}

func StartNewsProcessor() error {
	// Load processor config
	config, err := loadProcessorConfig("config.json")
	if err != nil {
		return fmt.Errorf("failed to load processor config: %v", err)
	}

	// Load data list
	dataList, err := loadDataList(config.DataList)
	if err != nil {
		return fmt.Errorf("failed to load data list: %v", err)
	}

	// Create worker pool with the number of items in the data list
	workerPool := utils.NewWorkerPool(5, len(dataList), processNewsItem)
	workerPool.Start()

	// Listen for alerts from data aggregator
	alertBuffer := make(chan kafka.Message)
	go utils.ConsumeToBuffer(alertBuffer, "alert_financials", "news-processor-group", "../../.env")

	// Process alerts and distribute work
	go func() {
		for msg := range alertBuffer {
			var request NewsRequest
			if err := json.Unmarshal(msg.Value, &request); err != nil {
				log.Printf("Error unmarshaling alert message: %v", err)
				continue
			}

			// Check if any of the segment targets match our configured segment
			segmentMatch := false
			for _, target := range request.SegmentTargets {
				if target == config.Segment {
					segmentMatch = true
					break
				}
			}

			// If no segment match, skip processing
			if !segmentMatch {
				continue
			}

			// Process all tickers from our data list
			log.Printf("Processing news for all tickers, triggered by segment: %s", config.Segment)
			for _, item := range dataList {
				workerPool.Submit(item.Value)
			}
		}
	}()

	// Process results
	inference_topic := "alert_llm"
	for result := range workerPool.ResultsChan {
		if result == nil {
			continue
		}

		newsData := result.(map[string]interface{})
		message := map[string]interface{}{
			"topic":           config.Topic,
			"segment":         config.Segment,
			"prompt_template": config.PromptTemplate,
			"ticker":          newsData["ticker"],
			"data":            newsData["data"],
			"timestamp":       time.Now().Unix(),
		}

		messageBytes, _ := json.Marshal(message)
		if err := utils.ProduceMessage(string(messageBytes), inference_topic); err != nil {
			log.Printf("Error sending result to RAG system: %v", err)
		}
	}

	return nil
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

func loadProcessorConfig(path string) (ProcessorConfig, error) {
	var configs []ProcessorConfig

	// Read the config file
	data, err := os.ReadFile(path)
	if err != nil {
		return ProcessorConfig{}, fmt.Errorf("error reading config file: %v", err)
	}

	// Parse JSON into slice of ProcessorConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return ProcessorConfig{}, fmt.Errorf("error parsing config JSON: %v", err)
	}

	// Validate we have at least one config
	if len(configs) == 0 {
		return ProcessorConfig{}, fmt.Errorf("no processor configs found in file")
	}

	// Use the first config (since we only need one for this processor)
	config := configs[0]

	// Validate required fields
	if config.Topic == "" {
		return ProcessorConfig{}, fmt.Errorf("topic is required in config")
	}
	if config.Segment == "" {
		return ProcessorConfig{}, fmt.Errorf("segment is required in config")
	}
	if config.DataList == "" {
		return ProcessorConfig{}, fmt.Errorf("data_list path is required in config")
	}

	return config, nil
}

func loadDataList(path string) (DataList, error) {
	var dataList DataList

	// Read the data list file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading data list file: %v", err)
	}

	// Parse JSON into DataList struct
	if err := json.Unmarshal(data, &dataList); err != nil {
		return nil, fmt.Errorf("error parsing data list JSON: %v", err)
	}

	// Validate that we have at least one item
	if len(dataList) == 0 {
		return nil, fmt.Errorf("data list must contain at least one item")
	}

	return dataList, nil
}

func main() {
	log.Println("Starting News Processor...")
	if err := StartNewsProcessor(); err != nil {
		log.Fatalf("Error starting news processor: %v", err)
	}
}
