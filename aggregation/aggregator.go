package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TopicRequest represents the request body schema for topic management
type TopicRequest struct {
	APIKey      string `json:"api_key" binding:"required"`
	Topic       string `json:"topic" binding:"required"`
	TopicAction string `json:"topic_action" binding:"required,oneof=create delete"`
}

// SegmentRequest represents the request body schema for segment management
type SegmentRequest struct {
	APIKey        string `json:"api_key" binding:"required"`
	Topic         string `json:"topic" binding:"required"`
	Segment       string `json:"segment" binding:"required"`
	SegmentAction string `json:"segment_action" binding:"required,oneof=create delete"`
}

// DataCollectionRequest represents the request body schema for data collection configuration
type DataCollectionRequest struct {
	Topic          string   `json:"topic" binding:"required"`
	SegmentTargets []string `json:"segment_targets" binding:"required,min=1"` // At least one segment required
}

// SetupEndpoints configures all endpoints
func SetupEndpoints(router *gin.Engine, apiKey string) {
	// Create a middleware to validate API key
	validateAPIKey := func(validKey string) gin.HandlerFunc {
		return func(c *gin.Context) {
			// Get API key from header
			headerKey := c.GetHeader("X-API-Key")
			if headerKey == "" {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Missing API key"})
				return
			}

			if headerKey != validKey {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid API key"})
				return
			}

			c.Next()
		}
	}

	// Apply the middleware to all endpoints
	router.POST("/api/topic", validateAPIKey(apiKey), handleTopicRequest)
	router.POST("/api/segment", validateAPIKey(apiKey), handleSegmentRequest)
	router.POST("/api/collect", validateAPIKey(apiKey), handleDataCollectionRequest)
}

func handleTopicRequest(c *gin.Context) {
	var request TopicRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format: " + err.Error()})
		return
	}

	// TODO: Validate API key here before proceeding

	switch request.TopicAction {
	case "create":
		if err := CreateKafkaTopicAndMongoDB(request.Topic); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "Failed to create topic and database",
				"details": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"message": "Topic and database created successfully",
			"topic":   request.Topic,
		})

	case "delete":
		// TODO: Implement delete functionality
		c.JSON(http.StatusNotImplemented, gin.H{
			"error": "Topic deletion not implemented yet",
		})
	}
}

func handleSegmentRequest(c *gin.Context) {
	var request SegmentRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format: " + err.Error()})
		return
	}

	// TODO: Validate API key here before proceeding

	switch request.SegmentAction {
	case "create":
		if err := createSegment(request.Topic, request.Segment); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "Failed to create segment",
				"details": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"message": "Segment created successfully",
			"topic":   request.Topic,
			"segment": request.Segment,
		})

	case "delete":
		if err := deleteSegment(request.Topic, request.Segment); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "Failed to delete segment",
				"details": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"message": "Segment deleted successfully",
			"topic":   request.Topic,
			"segment": request.Segment,
		})
	}
}

func handleDataCollectionRequest(c *gin.Context) {
	var request DataCollectionRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format: " + err.Error()})
		return
	}

	// Validate that the topic exists
	if err := validateTopicExists(request.Topic); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Topic validation failed: %v", err)})
		return
	}

	// Validate that all segments exist
	if err := validateSegmentsExist(request.Topic, request.SegmentTargets); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Segment validation failed: %v", err)})
		return
	}

	// Create message payload
	message := map[string]interface{}{
		"topic":           request.Topic,
		"segment_targets": request.SegmentTargets,
		"timestamp":       time.Now().Unix(),
	}

	// Convert message to JSON string
	messageBytes, err := json.Marshal(message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to marshal message: " + err.Error()})
		return
	}

	// Send message to Kafka
	if err := utils.ProduceMessage(string(messageBytes), request.Topic); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send message to Kafka: " + err.Error()})
		return
	}

	// Return success response
	c.JSON(http.StatusOK, gin.H{
		"message": "Data collection configuration accepted and published to Kafka",
		"config": gin.H{
			"topic":           request.Topic,
			"segment_targets": request.SegmentTargets,
		},
	})
}

func validateTopicExists(topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	mongoURI := "mongodb+srv://kobenaidun:fineas123@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Check if the database exists
	databases, err := client.ListDatabaseNames(ctx, bson.M{"name": topic})
	if err != nil {
		return fmt.Errorf("failed to list databases: %v", err)
	}
	if len(databases) == 0 {
		return fmt.Errorf("topic '%s' does not exist", topic)
	}

	return nil
}

func validateSegmentsExist(topic string, segments []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	mongoURI := "mongodb+srv://kobenaidun:fineas123@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Get the database
	db := client.Database(topic)

	// Check if all segments exist
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to list collections: %v", err)
	}

	// Create a map for O(1) lookup
	existingCollections := make(map[string]bool)
	for _, collection := range collections {
		existingCollections[collection] = true
	}

	// Validate each requested segment
	for _, segment := range segments {
		if !existingCollections[segment] {
			return fmt.Errorf("segment '%s' does not exist in topic '%s'", segment, topic)
		}
	}

	return nil
}

// CreateKafkaTopicAndMongoDB creates both a Kafka topic and corresponding MongoDB database
func CreateKafkaTopicAndMongoDB(topicName string) error {
	// Create Kafka topic
	if err := CreateKafkaTopic(topicName); err != nil {
		return err
	}

	// Create MongoDB database
	if err := createMongoDatabase(topicName); err != nil {
		// Note: In production, you might want to delete the Kafka topic if MongoDB creation fails
		return err
	}

	return nil
}

func createMongoDatabase(dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	mongoURI := "mongodb+srv://kobenaidun:fineas123@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)

	// Ping the database to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	// Create a collection in the new database to ensure it's created
	// MongoDB creates databases and collections automatically when you first store data
	db := client.Database(dbName)
	if err := db.CreateCollection(ctx, "segments"); err != nil {
		return err
	}

	log.Printf("MongoDB database '%s' created successfully", dbName)
	return nil
}

// CreateKafkaTopic dynamically checks for a Kafka topic and creates it if it doesn't exist.
// This is useful when auto.create.topics.enable is 'false'.
func CreateKafkaTopic(topicName string) error {

	//load environment variables
	/*
		KAFKA_BOOTSTRAP_SERVERS := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
		KAFKA_KEY := os.Getenv("KAFKA_KEY")
		KAFKA_SECRET := os.Getenv("KAFKA_SECRET")
	*/

	// Load Kafka configuration from your configuration source.
	kafkaConfig := utils.KafkaConfig{
		BootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),

		SASLUsername: os.Getenv("KAFKA_KEY"),
		SASLPassword: os.Getenv("KAFKA_SECRET"),
	}

	// Setup a custom dialer with TLS and SASL configuration.
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: kafkaConfig.SASLUsername,
			Password: kafkaConfig.SASLPassword,
		},
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	ctx := context.Background()

	// Connect to the Kafka bootstrap server using the secure dialer.
	conn, err := dialer.DialContext(ctx, "tcp", kafkaConfig.BootstrapServers)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Attempt to read partition metadata for the topic.
	// If the topic exists, ReadPartitions will return at least one partition.
	partitions, err := conn.ReadPartitions(topicName)
	if err == nil && len(partitions) > 0 {
		log.Printf("Topic %q already exists", topicName)
		return nil
	} else {
		log.Printf("Topic %q does not exist (error: %v), proceeding to create it", topicName, err)
	}

	// Retrieve the controller broker details.
	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	// Dial the controller broker using the secure dialer.
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := dialer.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	// Define the topic configuration.
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     1,
			ReplicationFactor: 3,
		},
	}

	// Create the topic by sending the configuration to the controller.
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	log.Printf("Topic %q created successfully", topicName)
	return nil
}

func createSegment(topic, segment string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	mongoURI := "mongodb+srv://kobenaidun:fineas123@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Check if the database exists
	databases, err := client.ListDatabaseNames(ctx, bson.M{"name": topic})
	if err != nil {
		return fmt.Errorf("failed to list databases: %v", err)
	}
	if len(databases) == 0 {
		return fmt.Errorf("topic (database) '%s' does not exist", topic)
	}

	// Create the segment (collection)
	db := client.Database(topic)
	err = db.CreateCollection(ctx, segment)
	if err != nil {
		// If the collection already exists, MongoDB returns an error
		if strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("segment '%s' already exists in topic '%s'", segment, topic)
		}
		return fmt.Errorf("failed to create segment: %v", err)
	}

	log.Printf("Created segment '%s' in topic '%s'", segment, topic)
	return nil
}

func deleteSegment(topic, segment string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to MongoDB
	mongoURI := "mongodb+srv://kobenaidun:fineas123@cluster0.z9znpv9.mongodb.net/?retryWrites=true&w=majority"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Check if the database exists
	databases, err := client.ListDatabaseNames(ctx, bson.M{"name": topic})
	if err != nil {
		return fmt.Errorf("failed to list databases: %v", err)
	}
	if len(databases) == 0 {
		return fmt.Errorf("topic (database) '%s' does not exist", topic)
	}

	// Delete the collection
	db := client.Database(topic)
	err = db.Collection(segment).Drop(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete segment: %v", err)
	}

	log.Printf("Deleted segment '%s' from topic '%s'", segment, topic)
	return nil
}


// To test the http requests
func main() {
    fmt.Println("Starting Aggregator Service (Mock Mode)...")

    // Mocking the endpoint instead of running the real server
    http.HandleFunc("/aggregator", func(w http.ResponseWriter, r *http.Request) {
        fmt.Println("Mock Aggregator: Received request")
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"message": "Mocked success"}`))
    })

    fmt.Println("Mock aggregator running on http://localhost:8080")
    http.ListenAndServe(":8080", nil)
}