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

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/segmentio/kafka-go"
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
	baseURL           = "https://api.finnworlds.com/api/v1"
	maxRetries        = 3
	retryDelay        = time.Second * 1
	defaultBatchSize  = 25
	defaultRateLimit  = 5
	kafkaBatchSize    = 50
	kafkaFlushTimeout = 100 * time.Millisecond
	apiKeyEnv         = "FINNWORLDS_API_KEY"
)

// startDescriptionProcessor initializes the processor with configuration, loads tickers,
// and begins listening to Kafka alerts. It coordinates batch execution, rate limiting,
// and sends processed messages to the LLM system.
func startDescriptionProcessor(ctx context.Context) error {
	config, err := loadProcessorConfig("config.json")
	if err != nil {
		return fmt.Errorf("config load failed: %w", err)
	}

	if config.BatchSize <= 0 {
		config.BatchSize = defaultBatchSize
	}

	tickerData, err := loadDataList(config.DataList)
	if err != nil {
		return fmt.Errorf("data list load failed: %w", err)
	}

	rateLimiter := rate.NewLimiter(rate.Limit(config.APIRateLimit), 1)
	kafkaWriter := utils.NewKafkaWriter("alert_llm")
	defer kafkaWriter.Close()

	alertChannel := make(chan kafka.Message)
	go utils.ConsumeToBuffer(alertChannel, config.Topic, "description-processor-group", "../../.env")

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

			var request DescriptionRequest
			if err := json.Unmarshal(alert.Value, &request); err != nil {
				utils.LogError("alert unmarshal failed", err)
				continue
			}

			if err := processSegment(ctx, config, tickerData, rateLimiter, kafkaWriter, request); err != nil {
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
	kafkaWriter *kafka.Writer,
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
		return nil
	}

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
		return nil
	}
	tickerBatches := buildTickerBatches(allTickers, config.BatchSize)

	// Initiating the workerpool
	workerPool := utils.NewWorkerPool(5, len(tickerBatches), func(job interface{}) interface{} {
		return processBatch(job.([]string), rateLimiter)
	})
	workerPool.Start()

	// Kafka buffering and main loop
	kafkaMessageBuffer := make([]kafka.Message, 0, kafkaBatchSize)
	flushTimer := time.NewTicker(kafkaFlushTimeout)
	defer flushTimer.Stop()

	for {
		select {
		// Results from workers
		case result, ok := <-workerPool.ResultsChan:
			if !ok {
				processingActive = false
				flushKafkaMessages(kafkaWriter, kafkaMessageBuffer)
				return nil
			}
			if result == nil {
				continue
			}

			batch := result.([]map[string]interface{})
			batchesProcessed++
			tickersProcessed += len(batch)

			for _, row := range batch {
				kafkaMessageBuffer = append(kafkaMessageBuffer, buildKafkaMessage(config, row))
				if len(kafkaMessageBuffer) >= kafkaBatchSize {
					flushKafkaMessages(kafkaWriter, kafkaMessageBuffer)
					kafkaMessageBuffer = kafkaMessageBuffer[:0]
				}
			}

		// Periodic flush
		case <-flushTimer.C:
			if len(kafkaMessageBuffer) > 0 {
				flushKafkaMessages(kafkaWriter, kafkaMessageBuffer)
				kafkaMessageBuffer = kafkaMessageBuffer[:0]
			}

		// Shutdown and cancellation
		case <-ctx.Done():
			log.Println("processSegment: context cancelled – flushing Kafka before exit")
			flushKafkaMessages(kafkaWriter, kafkaMessageBuffer)
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

	// Honour global rate limit before issuing the three API calls
	if err := rateLimiter.Wait(executionCtx); err != nil {
		return nil
	}

	// Fetch data from the three finnworld api endpoints
	informationMap := fetchAPIWithRetry(executionCtx, "information",    tickers)
	dividendsMap   := fetchAPIWithRetry(executionCtx, "dividends",      tickers)
	ratingsMap     := fetchAPIWithRetry(executionCtx, "companyratings", tickers)

	// Build per‑ticker payloads, guarding against nil maps
	processedTickers := make([]map[string]interface{}, 0, len(tickers))

	for _, ticker := range tickers {
		combinedData := make(map[string]interface{})

		// information
		if informationMap != nil {
			if entry, ok := informationMap[ticker]; ok {
				combinedData["information"] = entry
			}
		}
		// dividends
		if dividendsMap != nil {
			if entry, ok := dividendsMap[ticker]; ok {
				combinedData["dividends"] = entry
			}
		}
		// analyst ratings
		if ratingsMap != nil {
			if entry, ok := ratingsMap[ticker]; ok {
				combinedData["companyRatings"] = entry
			}
		}

		processedTickers = append(processedTickers, map[string]interface{}{
			"ticker": ticker,
			"data":   combinedData,
		})
	}

	return processedTickers
}

// fetchAPIWithRetry wraps fetchEndpointBatch with retry logic (3 attempts),
// returning nil if all attempts fail. Used to guard against transient network/API failures.
func fetchAPIWithRetry(ctx context.Context, endpoint string, tickers []string) map[string]interface{} {
	var lastError error
	for attempt := 0; attempt < maxRetries; attempt++ {
		result, err := fetchEndpointBatch(ctx, endpoint, tickers)
		if err == nil {
			return result
		}
		lastError = err
		time.Sleep(retryDelay)
	}
	utils.LogError("API fetch failed after retries", lastError)
	return nil
}

// fetchEndpointBatch sends a batched GET request to the specified Finnworlds endpoint,
// unmarshals the JSON response, and builds a ticker-keyed map of data objects.
// Returns only valid entries that include recognizable tickers.
func fetchEndpointBatch(ctx context.Context, endpoint string, tickers []string) (map[string]interface{}, error) {
    // Error handling #1: API key present
    apiKey := os.Getenv(apiKeyEnv)
    if apiKey == "" {
        return nil, fmt.Errorf("API key not set in %s", apiKeyEnv)
    }

    // Error handling #2: non‑empty ticker list
    if len(tickers) == 0 {
        return nil, nil
    }

    // Build and execute HTTP request
    httpClient := &http.Client{Timeout: 10 * time.Second}
    requestURL := fmt.Sprintf("%s/%s?key=%s&ticker=%s",
        baseURL, endpoint, apiKey, strings.Join(tickers, ","))

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
        return nil, fmt.Errorf("status %d", httpResponse.StatusCode)
    }

    // Parse JSON payload
    responseBytes, err := io.ReadAll(httpResponse.Body)
    if err != nil {
        return nil, err
    }

    var rawResponse map[string]interface{}
    if err := json.Unmarshal(responseBytes, &rawResponse); err != nil {
        return nil, err
    }

    resultArray, ok := extractResultsArray(rawResponse)
    if !ok {
        return nil, fmt.Errorf("invalid response format – ‘results’ array not found")
    }

    // Build ticker‑keyed map
    processedResults := make(map[string]interface{}, len(resultArray))
    for _, genericRecord := range resultArray {
        recordMap, isMap := genericRecord.(map[string]interface{})
        if !isMap || recordMap == nil {
            continue // skip malformed entries
        }

        tickerSymbol := extractTicker(recordMap)
        if tickerSymbol == "" {
            continue // skip if we can’t determine ticker
        }

        processedResults[tickerSymbol] = recordMap
    }

    return processedResults, nil
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
		"topic":           config.Topic,
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
