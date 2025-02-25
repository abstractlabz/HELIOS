package main

import (
	"bytes"
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
	Topic     string      `json:"topic"`
	Segment   string      `json:"segment"`
	Ticker    string      `json:"ticker"`
	Data      interface{} `json:"data"`
	Timestamp int64       `json:"timestamp"`
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

func callOpenAIAPI(prompt string) (string, error) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("OPENAI_API_KEY not set in environment")
	}

	url := "https://api.openai.com/v1/chat/completions"

	request := OpenAIRequest{
		Model: "gpt-4o", // Using GPT-4 Turbo, adjust as needed
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

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
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

	// Prepare prompt for OpenAI
	prompt := fmt.Sprintf("Analyze the following financial data for %s:\n%v\n\nPlease provide insights about:\n1. Key financial metrics and their implications\n2. Company's financial health\n3. Notable trends or changes\n4. Potential risks or opportunities",
		llmMessage.Ticker,
		llmMessage.Data)

	// Call OpenAI API
	analysis, err := callOpenAIAPI(prompt)
	if err != nil {
		log.Printf("Error calling OpenAI API: %v", err)
		return nil
	}

	// Log the analysis
	log.Printf("OpenAI Analysis for %s:\n%s", llmMessage.Ticker, analysis)

	// Create message for ingestor
	ingestorMessage := map[string]interface{}{
		"topic":     "alert_ingestor",
		"segment":   llmMessage.Segment,
		"ticker":    llmMessage.Ticker,
		"analysis":  analysis,
		"raw_data":  llmMessage.Data,
		"timestamp": time.Now().Unix(),
	}

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
	go utils.ConsumeToBuffer(alertBuffer, "alert_llm", "llm-processor-group", "../.env")

	// Process messages from the buffer
	for msg := range alertBuffer {
		workerPool.Submit(string(msg.Value))
	}

	return nil
}

func main() {
	// Load environment variables
	if err := godotenv.Load("../.env"); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	log.Println("Starting LLM Processor...")
	if err := StartLLMProcessor(); err != nil {
		log.Fatalf("Error starting LLM processor: %v", err)
	}
}
