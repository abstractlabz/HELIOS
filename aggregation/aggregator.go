package aggregation

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// CreateKafkaTopic dynamically checks for a Kafka topic and creates it if it doesn't exist.
// This is useful when auto.create.topics.enable is 'false'.
func CreateKafkaTopic(topicName string) error {

	//load environment variables
	/*
		KAFKA_BOOTSTRAP_SERVERS := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
		KAFKA_KEY := os.Getenv("KAFKA_KEY")
		KAFKA_SECRET := os.Getenv("KAFKA_SECRET")
	*/

	// Load Kafka configuration from your configuration source.
	kafkaConfig := utils.KafkaConfig{
		BootstrapServers: "pkc-p11xm.us-east-1.aws.confluent.cloud:9092",

		SASLUsername: "QRCT7SUSU7NG3NIY",
		SASLPassword: "ZZ6siYRsaHgdbTTGVZyZoe/1nhNJDB1p/of82PnyNQXKq0ZZ2UWnsieKl69jxZWt",
	}

	// Setup a custom dialer with TLS and SASL configuration.
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		SASLMechanism: plain.Mechanism{
			Username: kafkaConfig.SASLUsername,
			Password: kafkaConfig.SASLPassword,
		},
		TLS: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	ctx := context.Background()

	// Connect to the Kafka bootstrap server using the secure dialer.
	conn, err := dialer.DialContext(ctx, "tcp", kafkaConfig.BootstrapServers)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Attempt to read partition metadata for the topic.
	// If the topic exists, ReadPartitions will return at least one partition.
	partitions, err := conn.ReadPartitions(topicName)
	if err == nil && len(partitions) > 0 {
		log.Printf("Topic %q already exists", topicName)
		return nil
	} else {
		log.Printf("Topic %q does not exist (error: %v), proceeding to create it", topicName, err)
	}

	// Retrieve the controller broker details.
	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	// Dial the controller broker using the secure dialer.
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := dialer.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	// Define the topic configuration.
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     1,
			ReplicationFactor: 3,
		},
	}

	// Create the topic by sending the configuration to the controller.
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return err
	}

	log.Printf("Topic %q created successfully", topicName)
	return nil
}
