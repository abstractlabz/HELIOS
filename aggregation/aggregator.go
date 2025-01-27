package aggregation

import (
	"fmt"
	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/Shopify/sarama"
	"os"
)

// CreateKafkaTopic dynamically creates a Kafka topic
func CreateKafkaTopic(topicName string) error {
	// Load Kafka configuration from environment variables
	kafkaConfig := utils.KafkaConfig{
		BootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		SASLUsername:     os.Getenv("KAFKA_SASL_USERNAME"),
		SASLPassword:     os.Getenv("KAFKA_SASL_PASSWORD"),
		// CAPath:           os.Getenv("KAFKA_CA_CERT_PATH"),
		// CertPath:         os.Getenv("KAFKA_CERT_PATH"),
		// KeyPath:          os.Getenv("KAFKA_KEY_PATH"),
	}

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
