package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"math/big"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/joho/godotenv"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
	"github.com/segmentio/kafka-go"
)

type FinancialsRequest struct {
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

func processFinancialsItem(job interface{}) interface{} {
	ticker := strings.ToUpper(job.(string))
	// Remove any prefixes if present
	ticker = strings.TrimPrefix(ticker, "X:")
	ticker = strings.TrimPrefix(ticker, "I:")

	// Initialize Polygon client
	c := polygon.New(os.Getenv("POLYGON_API_KEY"))

	// Set up parameters for the financials request
	params := models.ListStockFinancialsParams{}.
		WithTicker(ticker).
		WithTimeframe("quarterly")
	// Make request to Polygon API
	iter := c.VX.ListStockFinancials(context.Background(), params)

	log.Printf("Making request to Polygon API for ticker: %s", ticker)

	// Check if there's data
	if !iter.Next() {
		if iter.Err() != nil {
			log.Printf("Error getting financials for ticker %s: %v", ticker, iter.Err())
		} else {
			log.Printf("No financial data found for ticker %s", ticker)
		}
		return nil
	}

	// Get the first financial statement
	financials := iter.Item()

	log.Printf("Received financials data for ticker %s: %+v", ticker, financials)

	// Convert financials to string for processing
	collection := fmt.Sprintf("%+v", financials)

	if collection == "" {
		log.Printf("Empty financial data for ticker %s", ticker)
		return nil
	}

	// Process the financial data
	targetSubstring := "equity_attributable_to_noncontrolling_interest"
	index := strings.Index(collection, targetSubstring)
	if index > 0 { // Only truncate if the target substring is not at the very beginning
		collection = collection[:index]
		collection = deleteNumberBeforeUSD(collection)
		accumulatedValues, err := accumulateFinancialValues(collection)
		if err != nil {
			log.Printf("Error processing financials for %s: %v", ticker, err)
			return nil
		}
		collection = accumulatedValues
	}

	log.Printf("Successfully processed financials for %s", ticker)
	return map[string]interface{}{
		"ticker": ticker,
		"data":   collection,
	}
}

func StartFinancialsProcessor() error {
	// Create a channel to listen for interrupt signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

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

	// Create worker pool
	workerPool := utils.NewWorkerPool(5, len(dataList), processFinancialsItem)
	workerPool.Start()

	// Listen for alerts from data aggregator
	alertBuffer := make(chan kafka.Message)
	go utils.ConsumeToBuffer(alertBuffer, "alert_financials", "financials-processor-group", "../../.env")

	// Add benchmarking variables
	startTime := time.Now()
	var tickersRequested int
	var tickersProcessed int
	processingActive := false

	// Create a function to output benchmark information
	outputBenchmark := func() {
		duration := time.Since(startTime)
		if tickersRequested > 0 {
			log.Printf("BENCHMARK: Processed %d/%d tickers in %v (%.2f tickers/second)",
				tickersProcessed, tickersRequested, duration, float64(tickersProcessed)/duration.Seconds())
		} else {
			log.Printf("BENCHMARK: No tickers were processed in this session")
		}
	}

	// Handle interrupt signals
	go func() {
		<-sigs
		log.Println("Received shutdown signal. Outputting final benchmark before exit...")
		outputBenchmark()
		os.Exit(0)
	}()

	// Process alerts and distribute work
	go func() {
		for msg := range alertBuffer {
			var request FinancialsRequest
			if err := json.Unmarshal(msg.Value, &request); err != nil {
				log.Printf("Error unmarshaling alert message: %v", err)
				continue
			}

			// Check if any segment targets match our configured segment
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

			// Start benchmarking when processing begins
			if !processingActive {
				startTime = time.Now()
				tickersRequested = 0
				tickersProcessed = 0
				processingActive = true
				log.Printf("Started processing benchmark at %v", startTime)
			}

			// Process all tickers from our data list
			log.Printf("Processing financials for all tickers, triggered by segment: %s", config.Segment)
			for _, item := range dataList {
				workerPool.Submit(item.Value)
				tickersRequested++
			}
		}
	}()

	// Process results
	inference_topic := "alert_llm"
	for result := range workerPool.ResultsChan {
		if processingActive {
			tickersProcessed++

			// If we've processed all tickers in this batch
			if tickersProcessed >= tickersRequested {
				outputBenchmark()

				// Reset for next batch
				processingActive = false
			}
		}

		if result == nil {
			continue
		}

		financialsData := result.(map[string]interface{})
		dataStr, ok := financialsData["data"].(string)
		if !ok || strings.TrimSpace(dataStr) == "" {
			log.Printf("Skipping ticker %s due to empty data", financialsData["ticker"])
			continue
		}

		message := map[string]interface{}{
			"topic":           config.Topic,
			"segment":         config.Segment,
			"prompt_template": config.PromptTemplate,
			"ticker":          financialsData["ticker"],
			"data":            dataStr,
			"timestamp":       time.Now().Unix(),
		}

		messageBytes, _ := json.Marshal(message)
		if err := utils.ProduceMessage(string(messageBytes), inference_topic); err != nil {
			log.Printf("Error sending result to RAG system: %v", err)
		}
	}

	return nil
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
	// Load environment variables
	if err := godotenv.Load("../../.env"); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	// Verify API key is set
	if os.Getenv("POLYGON_API_KEY") == "" {
		log.Fatal("POLYGON_API_KEY environment variable is not set")
	}

	log.Println("Starting Financials Processor...")
	if err := StartFinancialsProcessor(); err != nil {
		log.Fatalf("Error starting financials processor: %v", err)
	}
}

// Helper functions from your original code (accumulateFinancialValues, formatCurrency, etc.)
// would go here...

func deleteNumberBeforeUSD(input string) string {
	re := regexp.MustCompile(`(\d+)\s+USD`)
	output := re.ReplaceAllString(input, "USD")
	return output
}

func accumulateFinancialValues(input string) (string, error) {
	// Updated regex to match the formatted financials string.
	// It looks for "Label:" followed by the account name, then "Order:" (skipping over extra text),
	// then "Unit:USD" followed by "Value:" and the numerical value.
	re := regexp.MustCompile(`Label:\s*([^:]+?)\s+Order:.*?Unit:USD\s+Value:([0-9.eE+\-]+)`)
	matches := re.FindAllStringSubmatch(input, -1)

	if len(matches) == 0 {
		return "", fmt.Errorf("no financial values found in input")
	}

	accumulatedValues := make(map[string]float64)
	for _, match := range matches {
		account := strings.TrimSpace(match[1])
		valueStr := match[2]

		// Convert the number from scientific notation to a float64.
		f, _, err := big.ParseFloat(valueStr, 10, 64, big.ToNearestEven)
		if err != nil {
			return "", fmt.Errorf("error parsing value %s: %v", valueStr, err)
		}
		floatValue, _ := f.Float64()
		accumulatedValues[account] += floatValue
	}

	var result strings.Builder
	first := true
	for account, value := range accumulatedValues {
		if !first {
			result.WriteString(", ")
		} else {
			first = false
		}
		// Format the currency value with two decimals.
		formattedValue := fmt.Sprintf("$%.2f", value)
		parts := strings.Split(formattedValue, ".")
		integralPart := parts[0]

		// Insert commas in the integral part.
		n := len(integralPart)
		var integralPartWithCommas strings.Builder
		for i, ch := range integralPart {
			if i > 0 && (n-i)%3 == 0 && ch != '-' && ch != '$' {
				integralPartWithCommas.WriteString(",")
			}
			integralPartWithCommas.WriteRune(ch)
		}

		formattedValue = integralPartWithCommas.String() + "." + parts[1]
		result.WriteString(fmt.Sprintf(`"%s": "%s"`, account, formattedValue))
	}

	return result.String(), nil
}
