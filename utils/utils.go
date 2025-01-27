package utils

// Improving Security: Encrypt sensitive fields like SASLUsername, SASLPassword, or use a secure secret management solution.
// Certificate Validation: Consider additional validation for the certificates (e.g., expiration dates).
// Timeouts: Add timeout configurations for network operations to prevent indefinite hanging in edge cases.

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/Shopify/sarama"
)

// KafkaConfig holds credentials (configuration) for connecting to the Kafka cluster
// can make structs under a file in utils folder to change it in one place and can be used in other files !!
type KafkaConfig struct {
	BootstrapServers string //string containing the kafka bootstrap servers/broker addresses (host1:port1,host2:port2,host3:port3)
	SASLUsername     string //string containing the sasl username (should be encrypted)	(SASL = Simple Authentication and Security Layer)
	SASLPassword     string //string containing the sasl password (should be encrypted) (SASL = Simple Authentication and Security Layer)
	// CAPath           string //string containing the path to the ca certificate
	// CertPath         string //string containing the path to the client certificate
	// KeyPath          string //string containing the path to the client key	
}

// LoadKafkaConfig initializes Kafka producer with TLS and SASL authentication
// takes a KafkaConfig struct as input and returns a sarama.Config struct and an error	
func LoadKafkaConfig(cfg KafkaConfig) (*sarama.Config, error) {
	// Load the CA certificate
	caCert, err := ioutil.ReadFile(cfg.CAPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Load the user certificate and key
	cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate and key: %v", err)
	}

	// The & creates a pointer to a new tls.Config struct
	// Similar to C++, but Go handles memory management automatically
	// We need a pointer here because the Kafka client expects a *tls.Config (pointer)
	tlsConfig := &tls.Config{
		// Certificates: Your client's ID documents (like your passport)
		Certificates: []tls.Certificate{cert},

		// RootCAs: List of trusted certificate authorities
		RootCAs:      caCertPool,
		// Later, when establishing a connection:
		// 1. Server asks: "Show me your ID (certificate)"
		// 2. Server checks: "Is this ID issuer in my trusted list (RootCAs)?"
		// 3. If yes -> connection allowed, if no -> connection rejected
	}	

	// Initialize Sarama configuration with default settings
	config := sarama.NewConfig()
	
	// Enable TLS (Transport Layer Security) for encrypted network communication
	config.Net.TLS.Enable = true
	// Apply our custom TLS configuration (certificates and keys)
	config.Net.TLS.Config = tlsConfig
	
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
	
	// Make the producer wait for acknowledgment that messages were received
	// When true, the producer will return an error if the message wasn't successfully delivered
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
