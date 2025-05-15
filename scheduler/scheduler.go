package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

// ScheduleEntry represents an entry in the scheduling configuration file.
type ScheduleEntry struct {
	Topic    string  `json:"topic"`    // Topic name (e.g., "alert_financials")
	Segment  string  `json:"segment"`  // Segment name (e.g., "financials", "news")
	Schedule float64 `json:"schedule"` // Execution interval in days
}

// RequestPayload represents the data to be sent in the POST request.
type RequestPayload struct {
	Topic          string   `json:"topic"`
	SegmentTargets []string `json:"segment_targets"`
}

// lastRunTimes keeps track of when each scheduled task was last executed.
// Uses a mutex to protect lastRunTimes

var (
	lastRunTimes = make(map[string]time.Time) // Key is "topic:segment"
	mu           sync.Mutex
	apiEndpoint  = "http://localhost:8081/api/collect"
)

// getEntryKey generates a unique key for the entry for tracking last run times
func getEntryKey(entry ScheduleEntry) string {
	return fmt.Sprintf("%s:%s", entry.Topic, entry.Segment)
}

// LoadScheduleConfig reads a JSON file containing scheduled tasks and returns a list of ScheduleEntry.
func LoadScheduleConfig(filePath string) ([]ScheduleEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var schedules []ScheduleEntry
	if err := json.Unmarshal(data, &schedules); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return schedules, nil
}

// ShouldTrigger determines if a given ScheduleEntry should trigger a request based on its schedule.
func ShouldTrigger(entry ScheduleEntry) bool {
	mu.Lock()
	defer mu.Unlock()

	entryKey := getEntryKey(entry)
	lastRun, exists := lastRunTimes[entryKey]
	if !exists || lastRun.IsZero() {
		return true // First-time execution
	}

	nextRun := lastRun.Add(time.Duration(entry.Schedule) * 24 * time.Hour)
	return time.Now().After(nextRun)
}

// SendPostRequest prepares and sends a POST request to the API endpoint.
func SendPostRequest(entry ScheduleEntry, apiKey string) error {
	// Prepare request payload
	payload := RequestPayload{
		Topic:          entry.Topic,
		SegmentTargets: []string{entry.Segment},
	}

	// Convert payload to JSON
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request payload: %w", err)
	}

	// Retry mechanism for transient errors
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		// Create new request to add headers
		req, err := http.NewRequest("POST", apiEndpoint, bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		// Add required headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", apiKey)

		// Send the request
		client := &http.Client{}
		resp, err := client.Do(req)

		if err == nil {
			defer resp.Body.Close()

			// Log the response status and body
			body, _ := io.ReadAll(resp.Body)
			log.Printf("Response for %s:%s: Status=%d, Body=%s\n", entry.Topic, entry.Segment, resp.StatusCode, string(body))
			
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				entryKey := getEntryKey(entry)
				mu.Lock()
				lastRunTimes[entryKey] = time.Now()
				mu.Unlock()
				fmt.Printf("Triggered request for %s:%s\n", entry.Topic, entry.Segment)
				return nil
			}
			err = fmt.Errorf("received non-200 status code: %d", resp.StatusCode)
		}

		if i < maxRetries-1 {
			// Exponential backoff
			backoff := time.Duration(i+1) * time.Second * 2
			log.Printf("Retrying in %v: %v\n", backoff, err)
			time.Sleep(backoff)
		}
	}

	return fmt.Errorf("failed after %d retries", maxRetries)
}

// StartScheduler initializes the scheduling process. Has graceful shutdown functionality
func StartScheduler(scheduleFilePath string, apiKey string, stopChan chan struct{}) {
	schedules, err := LoadScheduleConfig(scheduleFilePath)
	if err != nil {
		log.Fatalf("Error loading schedule: %v\n", err) // Exit on critical error
	}

	for {
		select {
		case <-stopChan:
			log.Println("Shutting down scheduler gracefully...")
			return
		default:
			for _, entry := range schedules {
				if ShouldTrigger(entry) {
					go func(e ScheduleEntry) {
						if err := SendPostRequest(e, apiKey); err != nil {
							log.Printf("Failed to send request for %s:%s: %v\n", e.Topic, e.Segment, err)
						}
					}(entry)
				}
			}
			time.Sleep(1 * time.Minute)
		}
	}
	
}

// main is the entry point of the scheduler application.
func main() {

	// Load environment variables
	if err := godotenv.Load("../.env"); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	// Get API key from environment
	apiKey := os.Getenv("HELIOS_API_KEY")
	if apiKey == "" {
		log.Fatalf("HELIOS_API_KEY environment variable is not set")
	}
	// Default file path
	filePath := "./scheduler.json"

	// Check if a command-line argument is provided
	if len(os.Args) > 1 {
		filePath = os.Args[1] // Use the first argument as the file path
	}

	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Fatalf("Config file does not exist: %s\n", filePath)
	}

	// Channel to signal graceful shutdown
	stopChan := make(chan struct{})

	// Handle interrupt signals (e.g., Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
	<-sigChan
	log.Println("Received interrupt signal. Initiating shutdown...")
	close(stopChan)
	}()

	fmt.Println("Starting scheduler with config file:", filePath)
	StartScheduler(filePath, apiKey, stopChan)
}
