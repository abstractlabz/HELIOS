package main

import (
	"log"
	"os"

	"github.com/0xPCDefenders/HELIOS/aggregation"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

// FLOW FOR MAIN FUNCTION
// Create KafkaConfig struct (which matches the struct definition in utils.go)
// Pass that config to utils.CreateKafkaProducer()
// Use the producer to send messages

func main() {
	// Load environment variables from .env file
	err := godotenv.Load("../.env")
	if err != nil {
		log.Println("No .env file found. Proceeding with environment variables.")
	}

	// Load environment variables
	KAFKA_BOOTSTRAP_SERVERS := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	KAFKA_KEY := os.Getenv("KAFKA_KEY")
	KAFKA_SECRET := os.Getenv("KAFKA_SECRET")
	HELIOS_API_KEY := os.Getenv("HELIOS_API_KEY")
	log.Printf("Loaded API Key: %s", HELIOS_API_KEY)

	// Validate environment variables
	if KAFKA_BOOTSTRAP_SERVERS == "" || KAFKA_KEY == "" || KAFKA_SECRET == "" || HELIOS_API_KEY == "" {
		log.Fatal("One or more required environment variables are not set.")
	}

	// Initialize Gin router
	router := gin.Default()

	// Setup the topic management endpoint with API key
	aggregation.SetupEndpoints(router, HELIOS_API_KEY)

	// Start the server
	log.Println("Starting server on :8080")
	if err := router.Run(":8080"); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
