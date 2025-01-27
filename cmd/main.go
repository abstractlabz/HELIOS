package main

import (
	"fmt"
	"log"
	"os"
    "github.com/0xPCDefenders/HELIOS/utils" // not sure if this is correct	path to utils
    "github.com/Shopify/sarama"
	"github.com/0xPCDefenders/HELIOS/aggregation"
)

// FLOW FOR MAIN FUNCTION
// Create KafkaConfig struct (which matches the struct definition in utils.go)
// Pass that config to utils.CreateKafkaProducer()
// Use the producer to send messages

func main() {

	// 1.) Sets up the Kafka producer configuration, I'm not sure what actually information we have so 
	// I just put general stuff for now. The enviornment variables dont have all this.
    cfg := utils.KafkaConfig{
		// (NEEDS REAL VALUES)	
        BootstrapServers: os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
        SASLUsername:     os.Getenv("KAFKA_USERNAME"),
        SASLPassword:     os.Getenv("KAFKA_PASSWORD"),
        // CAPath:           os.Getenv("KAFKA_CA_PATH"),
        // CertPath:         os.Getenv("KAFKA_CERT_PATH"),
        // KeyPath:          os.Getenv("KAFKA_KEY_PATH"),
    }
    
    // 2. Create the producer using the config
    producer, err := utils.CreateKafkaProducer(cfg)
    if err != nil {
        panic(err)
    }
    defer producer.Close()
    
    // 3. Use the producer to send messages
    msg := &sarama.ProducerMessage{
        Topic: "your-topic", // (NEEDS REAL VALUES) the topic we want to send the message to	
        Value: sarama.StringEncoder("your message"), // (NEEDS REAL VALUES) the message we want to send	
    }
    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset) // prints the partition and offset of the message	

	err = aggregation.CreateKafkaTopic("test_topic")
	if err != nil {
		log.Fatalf("Failed to create Kafka topic: %v", err)
	}

	log.Println("Kafka topic created successfully!")
}