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

// Add this new type for streaming responses
type OpenAIStreamResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Content string `json:"content,omitempty"`
			Role    string `json:"role,omitempty"`
		} `json:"delta"`
		Index        int    `json:"index"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error,omitempty"`
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

func callOpenAIAPI(prompt string) (string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("OPENAI_API_KEY not set in environment")
	}

	url := "https://api.openai.com/v1/chat/completions"

	request := OpenAIRequest{
		Model: "gpt-4o",
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
		Temperature: 0.7,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("error marshaling request: %v", err)
	}

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			return "", fmt.Errorf("error creating request: %v", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Accept", "text/event-stream") // Add this for streaming

		client := &http.Client{Timeout: 300 * time.Second}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		req = req.WithContext(ctx)

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Attempt %d failed: %v", attempt, err)
			if attempt == maxRetries {
				return "", fmt.Errorf("max retries reached: %v", err)
			}
			time.Sleep(time.Second * time.Duration(attempt))
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("Attempt %d failed with status %d: %s", attempt, resp.StatusCode, string(body))
			if attempt == maxRetries {
				return "", fmt.Errorf("API request failed with status %d", resp.StatusCode)
			}
			time.Sleep(time.Second * time.Duration(attempt))
			continue
		}

		// Handle response
		//reader := bufio.NewReader(resp.Body)
		//var fullResponse strings.Builder

		log.Printf("Starting to read response for prompt length: %d", len(prompt))

		// Read the entire response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("error reading response body: %v", err)
		}

		// Log the raw response for debugging
		log.Printf("Raw response: %s", string(body))

		// Try parsing as a regular response first
		var response OpenAIResponse
		if err := json.Unmarshal(body, &response); err != nil {
			log.Printf("Error parsing regular response: %v", err)
			return "", fmt.Errorf("error parsing response: %v", err)
		}

		// Check if we have a valid response
		if len(response.Choices) > 0 && response.Choices[0].Message.Content != "" {
			content := response.Choices[0].Message.Content
			log.Printf("Successfully parsed response with length: %d", len(content))
			return content, nil
		}

		return "", fmt.Errorf("no valid content found in response")
	}

	return "", fmt.Errorf("failed after %d retries", maxRetries)
}

func processLLMMessage(job interface{}) interface{} {
	message := job.(string)
	log.Printf("Starting to process new message")

	var llmMessage LLMMessage
	if err := json.Unmarshal([]byte(message), &llmMessage); err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		return nil
	}

	log.Printf("Processing analysis request for ticker: %s", llmMessage.Ticker)

	prompt := fmt.Sprintf(llmMessage.PromptTemplate,
		llmMessage.Ticker,
		llmMessage.Data)

	// Get complete analysis from OpenAI
	analysis, err := callOpenAIAPI(prompt)
	if err != nil {
		log.Printf("Error getting OpenAI analysis for %s: %v", llmMessage.Ticker, err)
		return nil
	}

	log.Printf("Received complete analysis for %s (length: %d)", llmMessage.Ticker, len(analysis))

	// Create message for ingestor with complete analysis
	ingestorMessage := map[string]interface{}{
		"topic":     llmMessage.Topic,
		"segment":   llmMessage.Segment,
		"ticker":    llmMessage.Ticker,
		"analysis":  analysis,
		"raw_data":  llmMessage.Data,
		"timestamp": time.Now().Unix(),
	}

	messageBytes, err := json.Marshal(ingestorMessage)
	if err != nil {
		log.Printf("Error marshaling complete analysis for %s: %v", llmMessage.Ticker, err)
		return nil
	}

	log.Printf("Producing complete analysis to alert_ingestor for %s (message length: %d)",
		llmMessage.Ticker, len(messageBytes))

	if err := utils.ProduceMessage(string(messageBytes), "alert_ingestor"); err != nil {
		log.Printf("Failed to produce complete analysis to alert_ingestor for %s: %v",
			llmMessage.Ticker, err)
		return nil
	}

	log.Printf("Successfully produced complete analysis to alert_ingestor for %s", llmMessage.Ticker)
	return ingestorMessage
}

func StartLLMProcessor() error {
	log.Printf("Initializing LLM Processor with worker pool...")

	// Create worker pool with 10 workers
	workerPool := utils.NewWorkerPool(25, 500, processLLMMessage)
	workerPool.Start()

	log.Printf("Worker pool started. Creating Kafka consumer...")

	// Listen for messages from alert_llm topic
	alertBuffer := make(chan kafka.Message)
	go func() {
		log.Printf("Starting Kafka consumer for topic: alert_llm")
		utils.ConsumeToBuffer(alertBuffer, "alert_llm", "llm-processor-group", "../../.env")
	}()

	log.Printf("Kafka consumer started. Beginning message processing...")

	// Process messages from the buffer
	messageCount := 0
	for msg := range alertBuffer {
		messageCount++
		log.Printf("Received message #%d from alert_llm topic (length: %d)",
			messageCount, len(msg.Value))

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
