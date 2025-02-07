package main

import (
	"fmt"
	"log"
	"os"

	"github.com/0xPCDefenders/HELIOS/aggregation"
	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/IBM/sarama"
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

	// Validate environment variables
	if KAFKA_BOOTSTRAP_SERVERS == "" || KAFKA_KEY == "" || KAFKA_SECRET == "" {
		log.Fatal("One or more required environment variables are not set.")
	}

	cfg := utils.KafkaConfig{
		BootstrapServers: KAFKA_BOOTSTRAP_SERVERS,
		SASLUsername:     KAFKA_KEY,
		SASLPassword:     KAFKA_SECRET,
	}

	// 2. Create the topic first

	topicName := "des-10"
	err = aggregation.CreateKafkaTopic(topicName)
	if err != nil {
		panic(err)
	}

	// 3. Create the producer using the config
	producer, err := utils.CreateKafkaProducer(cfg)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	// 4. Send a message
	message := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder("Hello, World!"),
	}
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)

}
