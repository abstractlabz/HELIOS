package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto/tls"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"golang.org/x/time/rate"
)

// Configuration and data structures
type ProcessorConfig struct {
	Topic          string   `json:"topic"`
	Segment        string   `json:"segment"`
	DataList       string   `json:"data_list"`
	PromptTemplate string   `json:"prompt_template"`
	BatchSize      int      `json:"batch_size"`
	APIRateLimit   int      `json:"api_rate_limit"`
	Segments       []string `json:"segments"`
}

type DataList []struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type DescriptionRequest struct {
	Topic          string   `json:"topic"`
	SegmentTargets []string `json:"segment_targets"`
}

// These are subject to change based on actual kafka configurations
const (
	baseURL          = "https://api.finnworlds.com/api/v1"
	maxRetries       = 3
	retryDelay       = time.Second * 1
	defaultBatchSize = 14
	defaultRateLimit = 1
	maxWorkers       = 2
	apiKeyEnv        = "FINNWORLDS_API_KEY"
)

// startDescriptionProcessor initializes the processor with configuration, loads tickers,
// and begins listening to Kafka alerts. It coordinates batch execution, rate limiting,
// and sends processed messages to the LLM system.
func startDescriptionProcessor(ctx context.Context) error {
	log.Println("Starting description processor...")

	config, err := loadProcessorConfig("config.json")
	if err != nil {
		return fmt.Errorf("config load failed: %w", err)
	}
	log.Printf("Loaded configuration: topic=%s, segment=%s", config.Topic, config.Segment)

	if config.BatchSize <= 0 {
		config.BatchSize = defaultBatchSize
	}

	tickerData, err := loadDataList(config.DataList)
	if err != nil {
		return fmt.Errorf("data list load failed: %w", err)
	}
	log.Printf("Loaded %d tickers from data list", len(tickerData))

	rateLimiter := rate.NewLimiter(rate.Limit(config.APIRateLimit), 1)
	log.Println("Initialized rate limiter")

	alertChannel := make(chan kafka.Message)
	log.Println("Starting Kafka consumer...")
	go utils.ConsumeToBuffer(alertChannel, config.Topic, "description-processor-group", "../../.env")
	log.Println("Kafka consumer started, waiting for messages...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down processor loop (ctx cancelled)")
			return nil

		case alert, ok := <-alertChannel:
			if !ok {
				log.Println("Alert channel closed")
				return nil
			}
			log.Printf("Received alert message: %s", string(alert.Value))

			var request DescriptionRequest
			if err := json.Unmarshal(alert.Value, &request); err != nil {
				utils.LogError("alert unmarshal failed", err)
				continue
			}
			log.Printf("Processing request for segments: %v", request.SegmentTargets)

			if err := processSegment(ctx, config, tickerData, rateLimiter, request); err != nil {
				utils.LogError("segment processing failed", err)
			}
		}
	}
}

// processSegment filters alerts by segment, builds ticker batches, runs them through
// a worker pool, and asynchronously flushes enriched descriptions to Kafka.
func processSegment(
	ctx context.Context,
	config ProcessorConfig,
	tickerData DataList,
	rateLimiter *rate.Limiter,
	request DescriptionRequest,
) error {
	// Segment validation
	segmentMatch := false
	for _, target := range request.SegmentTargets {
		if target == config.Segment {
			segmentMatch = true
			break
		}
	}
	if !segmentMatch {
		log.Printf("Segment mismatch: got %v, want %s", request.SegmentTargets, config.Segment)
		return nil
	}
	log.Printf("Segment match confirmed, proceeding with processing")

	// Benchmark Setup
	startTime := time.Now()
	var batchesProcessed, tickersProcessed int
	processingActive := true
	defer func() {
		if processingActive {
			duration := time.Since(startTime)
			log.Printf(
				"BENCHMARK: Processed %d batches (%d tickers) in %v (%.2f tickers/sec)",
				batchesProcessed,
				tickersProcessed,
				duration,
				float64(tickersProcessed)/duration.Seconds(),
			)
		}
	}()

	// Build batches
	allTickers := getAllTickers(tickerData)
	if len(allTickers) == 0 {
		log.Println("No tickers found in data list")
		return nil
	}
	log.Printf("Building batches from %d tickers", len(allTickers))
	tickerBatches := buildTickerBatches(allTickers, config.BatchSize)
	log.Printf("Created %d batches of size %d", len(tickerBatches), config.BatchSize)

	// Initiating the workerpool with fewer workers
	log.Printf("Starting worker pool with %d workers", maxWorkers)
	workerPool := utils.NewWorkerPool(maxWorkers, len(tickerBatches), func(job interface{}) interface{} {
		return processBatch(job.([]string), rateLimiter)
	})
	workerPool.Start()

	// Submit jobs to worker pool
	log.Println("Submitting batches to worker pool")
	for _, batch := range tickerBatches {
		workerPool.Submit(batch)
	}
	log.Println("All batches submitted to worker pool")

	// Kafka buffering and main loop
	for {
		select {
		// Results from workers
		case result, ok := <-workerPool.ResultsChan:
			if !ok {
				log.Println("Worker pool results channel closed")
				processingActive = false
				return nil
			}
			if result == nil {
				log.Println("Received nil result from worker")
				continue
			}

			batch := result.([]map[string]interface{})
			batchesProcessed++
			tickersProcessed += len(batch)
			log.Printf("Processed batch %d/%d with %d tickers", batchesProcessed, len(tickerBatches), len(batch))

			// Process results
			inference_topic := "alert_llm"
			for _, row := range batch {
				message := map[string]interface{}{
					"topic":           config.Topic,
					"segment":         config.Segment,
					"prompt_template": config.PromptTemplate,
					"ticker":          row["ticker"],
					"data":            row["data"],
					"timestamp":       time.Now().Unix(),
				}
				messageBytes, _ := json.Marshal(message)
				if err := utils.ProduceMessage(string(messageBytes), inference_topic); err != nil {
					log.Printf("Error sending result to RAG system: %v", err)
				}
			}

		// Shutdown and cancellation
		case <-ctx.Done():
			log.Println("processSegment: context cancelled")
			processingActive = false // avoid double benchmark
			return nil
		}
	}
}

// processBatch handles one batch of tickers. It fetches company info, dividends,
// and analyst ratings concurrently, then builds per-ticker description payloads
// while checking for nil/empty maps to prevent panics.
func processBatch(tickers []string, rateLimiter *rate.Limiter) interface{} {
	// Hard time‑out for the entire batch
	executionCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Process each ticker individually
	processedTickers := make([]map[string]interface{}, 0, len(tickers))

	for _, ticker := range tickers {
		// Honour global rate limit before each API call
		if err := rateLimiter.Wait(executionCtx); err != nil {
			log.Printf("Rate limit wait failed for ticker %s: %v", ticker, err)
			continue
		}

		// Add delay between API calls to avoid rate limiting
		time.Sleep(2 * time.Second)

		// Fetch data from the three finnworld api endpoints
		information, err := fetchSingleTicker(executionCtx, "information", ticker)
		if err != nil {
			log.Printf("Failed to fetch information for %s: %v", ticker, err)
		}

		time.Sleep(2 * time.Second)

		dividends, err := fetchSingleTicker(executionCtx, "dividends", ticker)
		if err != nil {
			log.Printf("Failed to fetch dividends for %s: %v", ticker, err)
		}

		time.Sleep(2 * time.Second)

		ratings, err := fetchSingleTicker(executionCtx, "companyratings", ticker)
		if err != nil {
			log.Printf("Failed to fetch ratings for %s: %v", ticker, err)
		}

		// Build combined data for this ticker
		combinedData := make(map[string]interface{})
		if information != nil {
			combinedData["information"] = information
		}
		if dividends != nil {
			combinedData["dividends"] = dividends
		}
		if ratings != nil {
			combinedData["companyRatings"] = ratings
		}

		processedTickers = append(processedTickers, map[string]interface{}{
			"ticker": ticker,
			"data":   combinedData,
		})
	}

	return processedTickers
}

// fetchSingleTicker sends a GET request to the specified Finnworlds endpoint for a single ticker,
// unmarshals the JSON response, and returns the data object.
func fetchSingleTicker(ctx context.Context, endpoint string, ticker string) (map[string]interface{}, error) {
	// Error handling #1: API key present
	apiKey := os.Getenv(apiKeyEnv)
	if apiKey == "" {
		return nil, fmt.Errorf("API key not set in %s", apiKeyEnv)
	}

	// Error handling #2: non-empty ticker
	if ticker == "" {
		return nil, fmt.Errorf("empty ticker provided")
	}

	// Build and execute HTTP request
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// Add required parameters based on endpoint
	var requestURL string
	switch endpoint {
	case "information":
		requestURL = fmt.Sprintf("%s/%s?key=%s&ticker=%s",
			baseURL, endpoint, apiKey, ticker)
	case "dividends":
		// First get the dividends list
		requestURL = fmt.Sprintf("%s/dividendslist?key=%s&ticker=%s",
			baseURL, apiKey, ticker)
	case "companyratings":
		requestURL = fmt.Sprintf("%s/%s?key=%s&ticker=%s",
			baseURL, endpoint, apiKey, ticker)
	default:
		return nil, fmt.Errorf("unknown endpoint: %s", endpoint)
	}

	log.Printf("Making API request to: %s", requestURL)

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}

	httpResponse, err := httpClient.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer httpResponse.Body.Close()

	if httpResponse.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResponse.Body)
		return nil, fmt.Errorf("status %d: %s", httpResponse.StatusCode, string(body))
	}

	// Parse JSON payload
	responseBytes, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		return nil, err
	}

	log.Printf("Received API response: %s", string(responseBytes))

	var rawResponse map[string]interface{}
	if err := json.Unmarshal(responseBytes, &rawResponse); err != nil {
		return nil, fmt.Errorf("JSON unmarshal error: %v", err)
	}

	// Check for error in response
	if status, ok := rawResponse["status"].(map[string]interface{}); ok {
		if code, ok := status["code"].(float64); ok && code != 200 {
			message, _ := status["message"].(string)
			details, _ := status["details"].(string)
			return nil, fmt.Errorf("API error: %s - %s", message, details)
		}
	}

	// For dividends, we need to make a second request to get actual dividend data
	if endpoint == "dividends" {
		// Extract dividend IDs from the list response
		dividendList, ok := rawResponse["results"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid dividends list response format")
		}

		// Get actual dividend data for the ticker
		for _, item := range dividendList {
			if dividendMap, ok := item.(map[string]interface{}); ok {
				if id, ok := dividendMap["id"].(string); ok {
					// Make request for specific dividend data
					divURL := fmt.Sprintf("%s/dividends?key=%s&id=%s",
						baseURL, apiKey, id)

					divReq, err := http.NewRequestWithContext(ctx, http.MethodGet, divURL, nil)
					if err != nil {
						continue
					}

					divResp, err := httpClient.Do(divReq)
					if err != nil {
						continue
					}

					if divResp.StatusCode == http.StatusOK {
						divBody, _ := io.ReadAll(divResp.Body)
						var divData map[string]interface{}
						if err := json.Unmarshal(divBody, &divData); err == nil {
							divResp.Body.Close()
							return divData, nil
						}
					}
					divResp.Body.Close()
				}
			}
		}
		return nil, fmt.Errorf("no dividend data found for ticker %s", ticker)
	}

	// For information endpoint, extract data from the result field
	if endpoint == "information" {
		if result, ok := rawResponse["result"].(map[string]interface{}); ok {
			return result, nil
		}
		return nil, fmt.Errorf("invalid information response format")
	}

	// For company ratings, extract data from the result.output.analysts array
	if endpoint == "companyratings" {
		if result, ok := rawResponse["result"].(map[string]interface{}); ok {
			if output, ok := result["output"].(map[string]interface{}); ok {
				if analysts, ok := output["analysts"].([]interface{}); ok {
					return map[string]interface{}{
						"analysts":  analysts,
						"consensus": output["analyst_consensus"],
					}, nil
				}
			}
		}
		return nil, fmt.Errorf("invalid company ratings response format")
	}

	return nil, fmt.Errorf("no data found for ticker %s", ticker)
}

// Helper functions

// getAllTickers extracts all ticker symbols from the loaded DataList and uppercases them.
func getAllTickers(tickerData DataList) []string {
	tickers := make([]string, len(tickerData))
	for index, item := range tickerData {
		tickers[index] = strings.ToUpper(item.Value)
	}
	return tickers
}

// buildTickerBatches splits a slice of tickers into chunks of configurable batch size.
func buildTickerBatches(tickers []string, batchSize int) [][]string {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	var batches [][]string
	for startIndex := 0; startIndex < len(tickers); startIndex += batchSize {
		endIndex := startIndex + batchSize
		if endIndex > len(tickers) {
			endIndex = len(tickers)
		}
		batches = append(batches, tickers[startIndex:endIndex])
	}
	return batches
}

// extractResultsArray normalizes the different response shapes returned by
// various Finnworlds APIs, extracting the list of result objects (if present).
func extractResultsArray(response map[string]interface{}) ([]interface{}, bool) {
	if results, ok := response["results"].([]interface{}); ok {
		return results, true
	}
	if data, ok := response["data"].(map[string]interface{}); ok {
		if results, ok := data["results"].([]interface{}); ok {
			return results, true
		}
		if output, ok := data["output"].([]interface{}); ok {
			return output, true
		}
		if nestedData, ok := data["data"].([]interface{}); ok {
			return nestedData, true
		}
	}
	if result, ok := response["result"].(map[string]interface{}); ok {
		if results, ok := result["results"].([]interface{}); ok {
			return results, true
		}
	}
	return nil, false
}

// extractTicker attempts to determine the ticker symbol from a response object.
// It supports multiple field names depending on API schema.
func extractTicker(record map[string]interface{}) string {
	if ticker, ok := record["stock_ticker_symbol"].(string); ok {
		return strings.ToUpper(ticker)
	}
	if ticker, ok := record["ticker"].(string); ok {
		return strings.ToUpper(ticker)
	}
	if symbol, ok := record["symbol"].(string); ok {
		return strings.ToUpper(symbol)
	}
	return ""
}

// buildKafkaMessage constructs a structured Kafka message with metadata,
// timestamp, prompt template, and enriched data for the LLM processor.
func buildKafkaMessage(config ProcessorConfig, data map[string]interface{}) kafka.Message {
	message := map[string]interface{}{
		"topic":           "alert_llm",
		"segment":         config.Segment,
		"prompt_template": config.PromptTemplate,
		"ticker":          data["ticker"],
		"data":            data["data"],
		"timestamp":       time.Now().Unix(),
	}
	messageBytes, _ := json.Marshal(message)
	return kafka.Message{Value: messageBytes}
}

// flushKafkaMessages writes all buffered Kafka messages using the provided writer.
// It logs an error if the batch fails.
func flushKafkaMessages(writer *kafka.Writer, messages []kafka.Message) {
	if len(messages) == 0 {
		return
	}
	if err := writer.WriteMessages(context.Background(), messages...); err != nil {
		utils.LogError("kafka batch write failed", err)
	}
}

// loadProcessorConfig reads the processor's config.json file and unmarshals the
// first entry into a ProcessorConfig struct. Also applies default batch/rate settings.
func loadProcessorConfig(configPath string) (ProcessorConfig, error) {
	configFile, err := os.ReadFile(configPath)
	if err != nil {
		return ProcessorConfig{}, err
	}

	var configList []ProcessorConfig
	if err := json.Unmarshal(configFile, &configList); err != nil {
		return ProcessorConfig{}, err
	}

	if len(configList) == 0 {
		return ProcessorConfig{}, fmt.Errorf("no configurations found in %s", configPath)
	}

	config := configList[0]
	if config.BatchSize <= 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.APIRateLimit <= 0 {
		config.APIRateLimit = defaultRateLimit
	}

	return config, nil
}

// loadDataList loads the JSON ticker file (data_tickers.json) into a DataList,
// returning an error if the file is empty or invalid.
func loadDataList(dataPath string) (DataList, error) {
	dataFile, err := os.ReadFile(dataPath)
	if err != nil {
		return nil, err
	}

	var tickerData DataList
	if err := json.Unmarshal(dataFile, &tickerData); err != nil {
		return nil, err
	}

	if len(tickerData) == 0 {
		return nil, fmt.Errorf("no tickers found in %s", dataPath)
	}

	return tickerData, nil
}

// main sets up context cancellation for graceful shutdowns, listens for termination
// signals, and launches the description processor.
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Println("Shutdown signal received, exiting gracefully...")
		cancel()
	}()

	if err := startDescriptionProcessor(ctx); err != nil {
		utils.LogError("processor failed", err)
		os.Exit(1)
	}
}

// NewKafkaWriter creates a Kafka writer for the given topic.
func NewKafkaWriter(topic string) *kafka.Writer {
	broker := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if broker == "" {
		broker = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092" // fallback
	}

	dialer := &kafka.Dialer{
		SASLMechanism: plain.Mechanism{
			Username: os.Getenv("KAFKA_KEY"),
			Password: os.Getenv("KAFKA_SECRET"),
		},
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
		Dialer:  dialer,
	})
}
