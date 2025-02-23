package utils

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// ConsumeToBuffer reads messages from Kafka and sends them into the provided buffer channel.
func ConsumeToBuffer(buffer chan kafka.Message, topic string, groupID string, envPath string) {
	// Load environment variables from the .env file.
	err := godotenv.Load(envPath)
	if err != nil {

		log.Printf("Error loading .env file: %v", err)
		return
	}

	// SASL credentials from environment variables.
	kafkaUsername := os.Getenv("KAFKA_KEY")
	kafkaPassword := os.Getenv("KAFKA_SECRET")

	if kafkaUsername == "" || kafkaPassword == "" {
		log.Println("Kafka credentials are not set in the environment variables")
		return
	}

	saslMechanism := plain.Mechanism{
		Username: kafkaUsername,
		Password: kafkaPassword,
	}

	// Create a TLS config.
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	dialer := &kafka.Dialer{
		SASLMechanism: saslMechanism,
		TLS:           tlsConfig,
		Timeout:       10 * time.Second,
	}

	// Create a new Kafka reader with a GroupID and a short commit interval.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{os.Getenv("KAFKA_BOOTSTRAP_SERVERS")},
		Topic:    topic,
		GroupID:  groupID, // Added group ID for offset management.
		MaxBytes: 10e6,    // 10MB

		CommitInterval: 1 * time.Second, // Auto-commit offsets every 1 second.
		Dialer:         dialer,
	})

	// Continuously read messages and send them to the buffer.
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message from topic %s: %v", topic, err)
			// Continue reading even if one message errors.
			continue
		}
		buffer <- m
	}
	// (Note: r.Close() is never reached because of the infinite loop.)
}
