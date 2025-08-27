package utils

import (
	"log"
)

// KafkaConfig holds credentials (configuration) for connecting to the Kafka cluster.
type KafkaConfig struct {
	BootstrapServers string // Kafka broker addresses (host1:port1,host2:port2,...)
	SASLUsername     string // SASL username
	SASLPassword     string // SASL password
}

// LogError is a consistent error logging helper.
func LogError(message string, err error) {
	log.Printf("❌ %s: %v\n", message, err)
}
