package utils

import (
	"log"
	"fmt"
	"github.com/IBM/sarama"
)

// KafkaConfig holds credentials (configuration) for connecting to the Kafka cluster
// can make structs under a file in utils folder to change it in one place and can be used in other files !!
type KafkaConfig struct {
	BootstrapServers string //string containing the kafka bootstrap servers/broker addresses (host1:port1,host2:port2,host3:port3)
	SASLUsername     string //string containing the sasl username
	SASLPassword     string //string containing the sasl password
}

// LoadKafkaConfig initializes Kafka producer with SASL authentication
func LoadKafkaConfig(cfg KafkaConfig) (*sarama.Config, error) {
	// Initialize Sarama configuration with default settings
	config := sarama.NewConfig()
	
	// Enable SASL (Simple Authentication and Security Layer) for authentication
	config.Net.SASL.Enable = true
	// Set the username for SASL authentication
	config.Net.SASL.User = cfg.SASLUsername
	// Set the password for SASL authentication
	config.Net.SASL.Password = cfg.SASLPassword
	// Enable SASL handshake (the process where client and server agree on authentication method)
	config.Net.SASL.Handshake = true
	// Set the SASL mechanism to plaintext (username/password authentication)
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	// Enable TLS as it's required for SASL
	config.Net.TLS.Enable = true
	
	// Make the producer wait for acknowledgment that messages were received
	config.Producer.Return.Successes = true

	return config, nil
}

// CreateKafkaProducer initializes a Kafka producer with the provided configuration
func CreateKafkaProducer(cfg KafkaConfig) (sarama.SyncProducer, error) {
	config, err := LoadKafkaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load Kafka configuration: %v", err)
	}

	// Create the producer
	producer, err := sarama.NewSyncProducer([]string{cfg.BootstrapServers}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
	}

	log.Println("Kafka producer initialized successfully")
	return producer, nil
}


// The main "configuration" happens through the KafkaConfig struct that gets passed in from main.go. 
// The utils.go file is more about processing that configuration rather than containing values that need to be replaced.
// 	If you need to modify anything in utils.go, it would likely be the security settings (TLS/SASL) 
// based on your Kafka cluster's security requirements. For example, if your Kafka cluster doesn't use TLS, 
// you'd set config.Net.TLS.Enable = false.
// 	You'd also need to update the LoadKafkaConfig function to remove the TLS-related code.
// 	Remember, the KafkaConfig struct is just a way to pass configuration settings from main.go to utils.go, 
// and utils.go is responsible for processing those settings into a usable form for the Kafka producer.
