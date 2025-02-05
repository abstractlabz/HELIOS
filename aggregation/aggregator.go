package aggregation

import (
	"fmt"
	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/IBM/sarama"
	"os"
	"log"
)

// CreateKafkaTopic dynamically creates a Kafka topic by attempting to publish a test message to it.
// If the topic doesn't exist and auto.create.topics.enable is true in Kafka, the topic will be created.
// Returns an error if topic creation/message sending fails.
func CreateKafkaTopic(topicName string) error {
	// Load Kafka configuration from environment variables
	kafkaConfig := utils.KafkaConfig{
		BootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		SASLUsername:     os.Getenv("KAFKA_KEY"),           // using KAFKA_KEY as username
		SASLPassword:     os.Getenv("KAFKA_SECRET"),        // using KAFKA_SECRET as password
	}

	// Debug log
	log.Printf("Creating Kafka topic: %s\n", topicName)

	// Initialize Kafka producer
	producer, err := utils.CreateKafkaProducer(kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %v", err)
	}
	defer producer.Close()

	// Publish a message to the topic (as a test)
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder("Test message for topic creation"),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send test message: %v", err)
	}

	fmt.Printf("Kafka topic '%s' creation tested successfully\n", topicName)
	return nil
}
