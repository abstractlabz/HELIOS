package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	// Example Kafka library import; update to match your chosen library and version.
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/pinecone-io/go-pinecone/pinecone"

	// Import the worker pool from utils. Adjust module path to match your project structure.
	"github.com/0xPCDefenders/HELIOS/utils"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IngestorMessage matches the shape of JSON your inferencer produced.
type IngestorMessage struct {
	Analysis  string `json:"analysis"`
	RawData   string `json:"raw_data"`
	Segment   string `json:"segment"`
	Ticker    string `json:"ticker"`
	Timestamp int64  `json:"timestamp"`
	Topic     string `json:"topic"`
}

// Record represents one data item extracted from the input JSON.
type Record struct {
	ID          int    `json:"id"`
	Info        string `json:"info"`
	CurrentDate int    `json:"current_date"`
	Text        string `json:"text"`
}

// ingest is the worker function for the WorkerPool. It receives a Kafka
// message value (as []byte), parses it, and stores the relevant data in Pinecone.
func ingest(data interface{}) interface{} {
	msgBytes, ok := data.([]byte)
	if !ok {
		log.Println("[ingest] Unexpected data type; expected []byte.")
		return nil
	}

	// Parse the JSON as an IngestorMessage.
	var msg IngestorMessage
	if err := json.Unmarshal(msgBytes, &msg); err != nil {
		log.Printf("[ingest] Error parsing JSON: %v", err)
		return nil
	}

	// Store in MongoDB first
	ctx := context.Background()
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Println("[ingest] MONGO_URI environment variable not set")
		return nil
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Printf("[ingest] Failed to connect to MongoDB: %v", err)
		return nil
	}
	defer client.Disconnect(ctx)

	// Use the topic as the database name and segment as the collection name
	db := client.Database(msg.Topic)
	collection := db.Collection(msg.Segment)

	// Create a filter based on the ticker
	filter := bson.M{"ticker": msg.Ticker}

	// Create an update document
	update := bson.M{
		"$set": bson.M{
			"analysis":  msg.Analysis,
			"raw_data":  msg.RawData,
			"segment":   msg.Segment,
			"ticker":    msg.Ticker,
			"timestamp": msg.Timestamp,
			"topic":     msg.Topic,
		},
	}

	// Upsert the document (update if exists, insert if not)
	opts := options.Update().SetUpsert(true)
	result, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Printf("[ingest] Error upserting document to MongoDB: %v", err)
		return nil
	}

	log.Printf("[ingest] MongoDB upsert successful - Modified: %d, Upserted: %d", result.ModifiedCount, result.UpsertedCount)

	// Continue with Pinecone storage
	resp, err := IngestDataFromMsg(msg)
	if err != nil {
		log.Printf("[ingest] Error ingesting data to Pinecone: %v", err)
		return nil
	}

	log.Printf("[ingest] Successfully ingested data for ticker '%s' (segment: %s) to both MongoDB and Pinecone.", msg.Ticker, msg.Segment)
	return resp
}

// IngestDataFromMsg processes the message, chunks the text if you want chunking,
// obtains embeddings, and upserts to Pinecone.
func IngestDataFromMsg(msg IngestorMessage) (map[string]string, error) {
	// Example: Combine "analysis" + "raw_data" into one big text, or treat them separately
	combinedText := fmt.Sprintf("Analysis:\n%s\n\nRaw Data:\n%s", msg.Analysis, msg.RawData)

	// Create a slice with one "record" for chunking demonstration
	// (In reality, you might parse the raw_data further or store each piece distinctly.)
	record := Record{
		ID:          0,                // Some integer ID, or increment
		Info:        msg.Ticker,       // Or "segment" or some other
		CurrentDate: currentDateInt(), // Helper function below
		Text:        combinedText,
	}

	chunks := splitText(record.Text, 500) // Up to 500 runes each
	if len(chunks) == 0 {
		return nil, fmt.Errorf("empty text after chunking")
	}

	// Embed each chunk
	embeddings, err := embedTexts(chunks)
	if err != nil {
		return nil, fmt.Errorf("embedding error: %v", err)
	}
	if len(embeddings) != len(chunks) {
		return nil, fmt.Errorf("mismatch between chunks & embeddings")
	}

	// Build Pinecone vectors
	var vectors []*pinecone.Vector
	for i, chunk := range chunks {
		// Basic metadata: you can store anything you want
		metaMap := map[string]interface{}{
			"ticker":    msg.Ticker,
			"segment":   msg.Segment,
			"timestamp": msg.Timestamp,
			"chunk":     i,
			"text":      chunk,
		}
		metadata, err := structpb.NewStruct(metaMap)
		if err != nil {
			return nil, fmt.Errorf("metadata conversion error: %v", err)
		}

		vectors = append(vectors, &pinecone.Vector{
			Id:       uuid.New().String(),
			Values:   embeddings[i],
			Metadata: metadata,
		})
	}

	// Upsert to Pinecone
	ctx := context.Background()
	pc, err := pinecone.NewClient(pinecone.NewClientParams{
		// Replace with your real key or pull from env
		ApiKey: os.Getenv("PINECONE_API_KEY"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Pinecone client: %v", err)
	}

	indexName := "kafkatest" // or from an env var
	idx, err := pc.DescribeIndex(ctx, indexName)
	if err != nil {
		return nil, fmt.Errorf("failed to describe index: %v", err)
	}
	idxConn, err := pc.Index(pinecone.NewIndexConnParams{
		Host:      idx.Host,
		Namespace: "default",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create index connection: %v", err)
	}

	upsertCount, err := idxConn.UpsertVectors(ctx, vectors)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert vectors: %v", err)
	}

	// Return a summary
	return map[string]string{
		"status":   "200 OK",
		"upserted": fmt.Sprintf("%d", upsertCount),
		"ticker":   msg.Ticker,
	}, nil
}

func currentDateInt() int {
	// Convert YYYY-MM-DD -> YYYYMMDD int
	cds := time.Now().Format("2006-01-02")
	out, _ := strconv.Atoi(strings.ReplaceAll(cds, "-", ""))
	return out
}

// splitText splits a string into chunks of at most chunkSize runes.
func splitText(text string, chunkSize int) []string {
	var chunks []string
	runes := []rune(text)
	for i := 0; i < len(runes); i += chunkSize {
		end := i + chunkSize
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[i:end]))
	}
	return chunks
}

// embedTexts simulates calling an embedding API. Your logic might differ.
func embedTexts(texts []string) ([][]float32, error) {
	// This is an example using Pinecone's inference endpoint:
	ctx := context.Background()
	pc, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: os.Getenv("PINECONE_API_KEY"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Pinecone client: %v", err)
	}

	params := pinecone.EmbedParameters{InputType: "query", Truncate: "END"}
	embedReq := &pinecone.EmbedRequest{
		Model:      "multilingual-e5-large",
		TextInputs: texts,
		Parameters: params,
	}

	embedResp, err := pc.Inference.Embed(ctx, embedReq)
	if err != nil {
		return nil, fmt.Errorf("failed to embed texts: %v", err)
	}
	if embedResp.Data == nil || len(*embedResp.Data) == 0 {
		return nil, fmt.Errorf("no embedding data returned")
	}

	var result [][]float32
	for _, d := range *embedResp.Data {
		result = append(result, *d.Values)
	}
	return result, nil
}

// StartIngestor sets up a WorkerPool and uses utils.ConsumeToBuffer to read
// Kafka messages from the "alert_ingestor" topic.
func StartIngestor() error {
	// Create the worker pool (5 workers, 100 job buffer)
	workerPool := utils.NewWorkerPool(25, 500, ingest)
	workerPool.Start()

	// Pull Kafka messages into this channel
	msgBuffer := make(chan kafka.Message)

	// Start consuming messages from Kafka in a goroutine
	// pointing to your "alert_ingestor" topic, "alert_ingestor_group" group
	go utils.ConsumeToBuffer(
		msgBuffer,
		"alert_ingestor",
		"alert_ingestor_group",
		"../../.env", // Path to your .env if needed
	)

	// Read from the channel and submit jobs to the worker pool
	for msg := range msgBuffer {
		workerPool.Submit(msg.Value)
	}

	return nil
}

func main() {
	// Optionally load environment variables (if you use a .env file)
	if err := godotenv.Load("../../.env"); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	log.Println("Starting Ingestor Processor...")
	if err := StartIngestor(); err != nil {
		log.Fatalf("Error starting ingestor: %v", err)
	}
}
