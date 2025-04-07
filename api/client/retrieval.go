package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type PostDataInfo struct {
	Analysis  string `json:"analysis"`
	RawData   string `json:"raw_data"`
	Segment   string `json:"segment"`
	Ticker    string `json:"ticker"`
	Timestamp int64  `json:"timestamp"`
	Topic     string `json:"topic"`
}

func RetrieveData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse the request body
	var requestData struct {
		Topic   string `json:"topic"`
		Segment string `json:"segment"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
		return
	}

	// Get ticker from URL params
	ticker := r.URL.Query().Get("ticker")
	if ticker == "" {
		http.Error(w, "Ticker not provided", http.StatusBadRequest)
		return
	}

	// Connect to mongodb
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI("mongodb+srv://kobenaidun:fineas123@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority").SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		http.Error(w, "Database connection error", http.StatusInternalServerError)
		return
	}
	defer client.Disconnect(context.TODO())

	// Use the topic and segment from the request
	collection := client.Database(requestData.Topic).Collection(requestData.Segment)
	fmt.Println(requestData.Topic)
	fmt.Println(requestData.Segment)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var postDataInfo PostDataInfo
	// Find the document
	filter := bson.M{"ticker": ticker}
	err = collection.FindOne(ctx, filter).Decode(&postDataInfo)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, "{}")
			return
		}
		fmt.Printf("Database error: %v\nFilter: %v\nTopic: %s\nSegment: %s\n",
			err, filter, requestData.Topic, requestData.Segment)
		http.Error(w, "Error fetching data from database", http.StatusInternalServerError)
		return
	}

	// Convert the result to JSON and return it
	jsonData, err := json.Marshal(postDataInfo)
	if err != nil {
		http.Error(w, "Error marshalling data to JSON", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
}

func main() {
	// Set up HTTP router
	http.HandleFunc("/retrieve", RetrieveData)

	// Start the server on port 8081
	fmt.Println("Server starting on :8035")
	if err := http.ListenAndServe(":8035", nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
		os.Exit(1)
	}
}
