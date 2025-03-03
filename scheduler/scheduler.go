package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// ScheduleEntry represents an entry in the scheduling configuration file.
type ScheduleEntry struct {
	DataCollectionFilePath string `json:"data_collection_file_path"` // Path to the data file
	Schedule              int    `json:"schedule"`                   // Execution interval in days
}

// lastRunTimes keeps track of when each scheduled task was last executed.
// Uses a mutex to protect lastRunTimes
var (
	lastRunTimes = make(map[string]time.Time)
	mu           sync.Mutex
)

// LoadScheduleConfig reads a JSON file containing scheduled tasks and returns a list of ScheduleEntry.
func LoadScheduleConfig(filePath string) ([]ScheduleEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
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

	lastRun, exists := lastRunTimes[entry.DataCollectionFilePath]
	if !exists || lastRun.IsZero() {
		return true // First-time execution
	}

	nextRun := lastRun.Add(time.Duration(entry.Schedule) * 24 * time.Hour
	return time.Now().After(nextRun)
}

// SendPostRequest reads the data collection file and sends a POST request to the aggregator endpoint.
func SendPostRequest(entry ScheduleEntry) error {
	data, err := ioutil.ReadFile(entry.DataCollectionFilePath)
	if err != nil {
		return fmt.Errorf("failed to read data file: %w", err)
	}

	// Retry mechanism for transient errors
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Post("http://localhost:8080/aggregator", "application/json", bytes.NewBuffer(data))
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				mu.Lock()
				lastRunTimes[entry.DataCollectionFilePath] = time.Now()
				mu.Unlock()
				fmt.Printf("Triggered request for %s\n", entry.DataCollectionFilePath)
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

	return fmt.Errorf("failed after %d retries: %w", maxRetries, err)
}

// StartScheduler initializes the scheduling process.
func StartScheduler(scheduleFilePath string) {
	schedules, err := LoadScheduleConfig(scheduleFilePath)
	if err != nil {
		// Exit on critical error
		log.Fatalf("Error loading schedule: %v\n", err)
	}

	for {
		for _, entry := range schedules {
			if ShouldTrigger(entry) {
				// Run requests concurrently
				go func(e ScheduleEntry) {
					if err := SendPostRequest(e); err != nil {
						log.Printf("Failed to send request for %s: %v\n", e.DataCollectionFilePath, err)
					}
				}(entry)
			}
		}
		// Check every minute
		time.Sleep(1 * time.Minute)
	}
}

// main is the entry point of the scheduler application.
func main() {
	fmt.Println("Starting scheduler...")
	StartScheduler("scheduler/scheduler.json")
}