package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type LLMMessage struct {
	Topic          string      `json:"topic"`
	Segment        string      `json:"segment"`
	Ticker         string      `json:"ticker"`
	Data           interface{} `json:"data"`
	Timestamp      int64       `json:"timestamp"`
	PromptTemplate string      `json:"prompt_template"`
}

type OpenAIRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OpenAIResponse struct {
	Choices []struct {
		Message ChatMessage `json:"message"`
	} `json:"choices"`
}

type DeepSeekRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

func callDeepSeekAPI(prompt string) (string, error) {
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("DEEPSEEK_API_KEY not set in environment")
	}

	url := "https://api.deepseek.com/chat/completions"

	request := DeepSeekRequest{
		Model: "deepseek-chat",
		Messages: []ChatMessage{
			{
				Role:    "system",
				Content: "You are a financial analyst. Analyze the following financial data and provide detailed insights about the company's financial health, key metrics, and potential risks or opportunities.",
			},
			{
				Role:    "user",
				Content: prompt,
			},
		},
		Stream: false,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("error marshaling request: %v", err)
	}

	// Implement retry logic
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			return "", fmt.Errorf("error creating request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)

		// Increased timeout to 90 seconds
		client := &http.Client{Timeout: 90 * time.Second}

		// Add context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		req = req.WithContext(ctx)

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Attempt %d failed: %v", attempt, err)
			if attempt == maxRetries {
				return "", fmt.Errorf("max retries reached: %v", err)
			}
			time.Sleep(time.Second * time.Duration(attempt)) // Exponential backoff
			continue
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Attempt %d failed reading body: %v", attempt, err)
			if attempt == maxRetries {
				return "", fmt.Errorf("max retries reached: %v", err)
			}
			time.Sleep(time.Second * time.Duration(attempt))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Attempt %d failed with status %d: %s", attempt, resp.StatusCode, string(body))
			if attempt == maxRetries {
				return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
			}
			time.Sleep(time.Second * time.Duration(attempt))
			continue
		}

		var response OpenAIResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return "", fmt.Errorf("error unmarshaling response: %v", err)
		}

		if len(response.Choices) == 0 {
			return "", fmt.Errorf("no response choices received")
		}

		return response.Choices[0].Message.Content, nil
	}

	return "", fmt.Errorf("failed after %d retries", maxRetries)
}

func processLLMMessage(job interface{}) interface{} {
	message := job.(string)

	// Parse the message
	var llmMessage LLMMessage
	if err := json.Unmarshal([]byte(message), &llmMessage); err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		return nil
	}

	// Log the incoming message
	log.Printf("Processing message for ticker %s from segment %s",
		llmMessage.Ticker,
		llmMessage.Segment)

	// Prepare prompt for DeepSeek
	prompt := fmt.Sprintf(llmMessage.PromptTemplate,
		llmMessage.Ticker,
		llmMessage.Data)

	// Call DeepSeek API
	analysis, err := callDeepSeekAPI(prompt)
	if err != nil {
		log.Printf("Error calling DeepSeek API: %v", err)
		return nil
	}

	// Log the analysis
	log.Printf("DeepSeek Analysis for %s:\n%s", llmMessage.Ticker, analysis)

	// Create message for ingestor
	ingestorMessage := map[string]interface{}{
		"topic":     llmMessage.Topic,
		"segment":   llmMessage.Segment,
		"ticker":    llmMessage.Ticker,
		"analysis":  analysis,
		"raw_data":  llmMessage.Data,
		"timestamp": time.Now().Unix(),
	}
	log.Println(llmMessage.Topic)

	// Convert to JSON
	messageBytes, err := json.Marshal(ingestorMessage)
	if err != nil {
		log.Printf("Error marshaling ingestor message: %v", err)
		return nil
	}

	// Produce message to alert_ingestor topic
	if err := utils.ProduceMessage(string(messageBytes), "alert_ingestor"); err != nil {
		log.Printf("Error producing message to alert_ingestor: %v", err)
		return nil
	}

	log.Printf("Successfully produced analysis to alert_ingestor for ticker %s", llmMessage.Ticker)

	return ingestorMessage
}

func StartLLMProcessor() error {
	// Create worker pool with 5 workers
	workerPool := utils.NewWorkerPool(5, 100, processLLMMessage)
	workerPool.Start()

	// Listen for messages from alert_llm topic
	alertBuffer := make(chan kafka.Message)
	go utils.ConsumeToBuffer(alertBuffer, "alert_llm", "llm-processor-group", "../../.env")

	// Process messages from the buffer
	for msg := range alertBuffer {
		workerPool.Submit(string(msg.Value))
	}

	return nil
}

func main() {
	// Load environment variables
	if err := godotenv.Load("../../.env"); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	log.Println("Starting LLM Processor...")
	if err := StartLLMProcessor(); err != nil {
		log.Fatalf("Error starting LLM processor: %v", err)
	}
}
