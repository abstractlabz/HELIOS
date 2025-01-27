package main

import (
	"fmt"
	"os"
	"github.com/0xPCDefenders/HELIOS/utils"
	"github.com/IBM/sarama"
	"github.com/0xPCDefenders/HELIOS/aggregation"
)

// FLOW FOR MAIN FUNCTION
// Create KafkaConfig struct (which matches the struct definition in utils.go)
// Pass that config to utils.CreateKafkaProducer()
// Use the producer to send messages

func main() {
    // 1.) Sets up the Kafka producer configuration
    cfg := utils.KafkaConfig{
        BootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
        SASLUsername:     os.Getenv("KAFKA_KEY"),
        SASLPassword:     os.Getenv("KAFKA_SECRET"),
    }
    
    // 2. Create the topic first
    topicName := "test_topic"
    err := aggregation.CreateKafkaTopic(topicName)
    if err != nil {
        panic(err)
    }

    // 3. Create the producer using the config
    producer, err := utils.CreateKafkaProducer(cfg)
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    // 4. Send test message
    msg := &sarama.ProducerMessage{
        Topic: topicName,
        Value: sarama.StringEncoder("Hello, Kafka!"),
    }
    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}