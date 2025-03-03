// Package main implements a simple scheduler that reads a JSON configuration file,
// checks if a scheduled task should be executed, and triggers an HTTP request if necessary.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

// ScheduleEntry represents an entry in the scheduling configuration file.
// It specifies the path to a data collection JSON file and the execution frequency in days.
type ScheduleEntry struct {
	DataCollectionFilePath string `json:"data_collection_file_path"` // Path to the data file
	Schedule              int    `json:"schedule"`                   // Execution interval in days
}

// lastRunTimes keeps track of when each scheduled task was last executed.
var lastRunTimes = make(map[string]time.Time)

// LoadScheduleConfig reads a JSON file containing scheduled tasks and returns a list of ScheduleEntry.
// The file should be formatted as an array of objects with `data_collection_file_path` and `schedule` fields.
func LoadScheduleConfig(filePath string) ([]ScheduleEntry, error) {
	// Open the configuration file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read and parse JSON data
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var schedules []ScheduleEntry
	err = json.Unmarshal(data, &schedules)
	if err != nil {
		return nil, err
	}

	return schedules, nil
}

// ShouldTrigger determines if a given ScheduleEntry should trigger a request based on its schedule.
// It checks the last execution time and compares it with the current time.
func ShouldTrigger(entry ScheduleEntry) bool {
	lastRun, exists := lastRunTimes[entry.DataCollectionFilePath]

	if !exists || lastRun.IsZero() {
		return true // First-time execution
	}

	nextRun := lastRun.Add(time.Duration(entry.Schedule) * 24 * time.Hour)
	return time.Now().After(nextRun)
}

// SendPostRequest reads the data collection file and sends a POST request to the aggregator endpoint.
func SendPostRequest(entry ScheduleEntry) error {
	data, err := ioutil.ReadFile(entry.DataCollectionFilePath)
	if err != nil {
		return err
	}

	resp, err := http.Post("http://localhost:8080/aggregator", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	fmt.Printf("Triggered request for %s\n", entry.DataCollectionFilePath)
	lastRunTimes[entry.DataCollectionFilePath] = time.Now()
	return nil
}

// StartScheduler initializes the scheduling process by loading the JSON configuration file
// and periodically checking whether any tasks need to be triggered.
func StartScheduler(scheduleFilePath string) {
	schedules, err := LoadScheduleConfig(scheduleFilePath)
	if err != nil {
		fmt.Println("Error loading schedule:", err)
		return
	}

	for {
		for _, entry := range schedules {
			if ShouldTrigger(entry) {
				go func(e ScheduleEntry) { // Run requests concurrently
					if err := SendPostRequest(e); err != nil {
						fmt.Println("Failed to send request:", err)
					}
				}(entry)
			}
		}
		time.Sleep(1 * time.Minute) // Check every minute
	}
}

// main is the entry point of the scheduler application.
// It starts the scheduler using a predefined JSON file.
func main() {
	fmt.Println("Starting scheduler...")
	StartScheduler("scheduler/scheduler.json")
}
