package utils

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
)

// KafkaConfig holds credentials (configuration) for connecting to the Kafka cluster.
type KafkaConfig struct {
	BootstrapServers string // Kafka broker addresses (host1:port1,host2:port2,...)
	SASLUsername     string // SASL username
	SASLPassword     string // SASL password
}

// LoadKafkaConfig initializes Kafka producer configuration with SASL and TLS settings.
func LoadKafkaConfig(cfg KafkaConfig) (*sarama.Config, error) {
	config := sarama.NewConfig()

	// Enable SASL authentication.
	config.Net.SASL.Enable = true
	config.Net.SASL.User = cfg.SASLUsername
	config.Net.SASL.Password = cfg.SASLPassword
	config.Net.SASL.Handshake = true
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	// Enable TLS (for testing, InsecureSkipVerify is enabled).
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: true, // WARNING: Only for testing; set proper verification in production.
	}

	// Additional recommended configurations.
	config.Version = sarama.V2_8_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Net.MaxOpenRequests = 1

	config.Metadata.Full = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	return config, nil
}

// CreateKafkaProducer initializes a Kafka producer with the provided configuration.
func CreateKafkaProducer(cfg KafkaConfig) (sarama.SyncProducer, error) {
	config, err := LoadKafkaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load Kafka configuration: %v", err)
	}

	log.Println("Attempting to create Kafka producer...")

	producer, err := sarama.NewSyncProducer([]string{cfg.BootstrapServers}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
	}

	log.Println("Kafka producer initialized successfully")
	return producer, nil
}

// NewKafkaWriter creates a Kafka writer for the given topic.
func NewKafkaWriter(topic string) *kafka.Writer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092" // fallback
	}
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
		Async:   false,
	})
}

// LogError is a consistent error logging helper.
func LogError(message string, err error) {
	log.Printf("❌ %s: %v\n", message, err)
}
