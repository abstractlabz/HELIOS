package utils

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ProduceDocument produces a Kafka message where the key is the document ID
// (converted to a hex string) and the value is the entire document in JSON format.
func ProduceDocument(docID primitive.ObjectID, doc bson.M, topic string) {
	// Marshal the document into JSON.
	jsonData, err := json.Marshal(doc)
	if err != nil {
		log.Printf("Error marshalling document for id %s: %v\n", docID.Hex(), err)
		return
	}

	// Set up the SASL mechanism using environment variables.
	saslMechanism := plain.Mechanism{
		Username: os.Getenv("KAFKA_KEY"),
		Password: os.Getenv("KAFKA_SECRET"),
	}

	// Set up TLS configuration.
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Create a dialer that supports SASL/PLAIN over TLS.
	dialer := &kafka.Dialer{
		SASLMechanism: saslMechanism,
		TLS:           tlsConfig,
	}

	// Retrieve the Kafka bootstrap server from the environment.
	kafkaBroker := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")

	// Connect to the Kafka leader for topic "alert_normalize" on a random partition.
	conn, err := dialer.DialLeader(
		context.Background(),
		"tcp",
		kafkaBroker,
		topic,
		rand.Intn(5), // Randomly select a partition.

	)
	if err != nil {
		log.Printf("Error connecting to Kafka: %v\n", err)
		return
	}
	defer conn.Close()

	// Create the Kafka message with the document ID as key and the JSON document as value.
	msg := kafka.Message{
		Key:   []byte(docID.Hex()),
		Value: []byte(jsonData),
	}

	// Write the message to Kafka.
	if _, err := conn.WriteMessages(msg); err != nil {
		log.Printf("Error writing message to Kafka for docID %s: %v\n", docID.Hex(), err)
		return
	}

	fmt.Printf("Message written successfully for docID %s to Kafka.\n", docID.Hex())
}

// ProduceMessage sends a string message to the specified Kafka topic
func ProduceMessage(message string, topic string) error {
	// Set up the SASL mechanism using hardcoded credentials
	saslMechanism := plain.Mechanism{
		Username: "QRCT7SUSU7NG3NIY",
		Password: "ZZ6siYRsaHgdbTTGVZyZoe/1nhNJDB1p/of82PnyNQXKq0ZZ2UWnsieKl69jxZWt",
	}

	// Set up TLS configuration.
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Create a dialer that supports SASL/PLAIN over TLS.
	dialer := &kafka.Dialer{
		SASLMechanism: saslMechanism,
		TLS:           tlsConfig,
	}

	// Use hardcoded Kafka broker
	kafkaBroker := "pkc-p11xm.us-east-1.aws.confluent.cloud:9092"

	// Connect to the Kafka leader for the specified topic
	conn, err := dialer.DialLeader(
		context.Background(),
		"tcp",
		kafkaBroker,
		topic,
		0, // Use partition 0
	)
	if err != nil {
		log.Printf("Error connecting to Kafka: %v\n", err)
		return err
	}
	defer conn.Close()

	// Create and write the Kafka message
	msg := kafka.Message{
		Value: []byte(message),
	}

	if _, err := conn.WriteMessages(msg); err != nil {
		log.Printf("Error writing message to Kafka: %v\n", err)
		return err
	}

	log.Printf("Message written successfully to topic %s\n", topic)
	return nil
}
