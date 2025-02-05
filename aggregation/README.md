# Aggregation Module

## Overview

The `aggregation` module is responsible for dynamically managing **Kafka topics** and their corresponding **MongoDB collections** based on incoming JSON requests. This ensures the efficient organization of raw data into **topics** (high-level categories) and **segments** (subcategories).

---

## Functionality

### Topic Management
- **Create Kafka Topics**: Dynamically create Kafka topics based on JSON requests.
- **Delete Kafka Topics**: Remove topics and their corresponding MongoDB collections when no longer needed.

### MongoDB Integration
- **Map Kafka Topics to MongoDB Collections**: Each topic is mapped to a MongoDB collection, enabling long-term storage and querying of associated data.

---

## Design Recipe

### Example Function: `CreateKafkaTopic`

#### **Purpose**
Creates a new Kafka topic with the specified name and ensures the topic doesn't already exist. Also creates a MongoDB collection to store data associated with the topic.

#### **Signature**
```go
func CreateKafkaTopic(topicName string) error
```

#### **Inputs**
- `topicName (string)`: The name of the Kafka topic to create.

#### **Outputs**
- `error`: Returns an error if the topic creation fails or the topic already exists.

#### **Example Usage**
```go
err := CreateKafkaTopic("financial_info")
if err != nil {
    log.Fatal(err)
}
```

---

## Utilities Directory

### Kafka Configuration and Producer Setup

The `utils` package handles the configuration and creation of Kafka producers.

#### **KafkaConfig Struct**
```go:utils/utils.go
package utils

import (
    "fmt"
    "log"

    "github.com/IBM/sarama"
)

// KafkaConfig holds the configuration parameters for connecting to the Kafka cluster.
// It is populated from environment variables in main.go and processed by utils.go.
type KafkaConfig struct {
    BootstrapServers string // Comma-separated list of Kafka bootstrap servers (e.g., "host1:port1,host2:port2")
    SASLUsername     string // SASL username for authentication
    SASLPassword     string // SASL password for authentication
}
```

#### **LoadKafkaConfig Function**
```go:utils/utils.go
// LoadKafkaConfig initializes the Kafka configuration using the provided KafkaConfig.
// It sets up SASL authentication and TLS as required.
func LoadKafkaConfig(cfg KafkaConfig) (*sarama.Config, error) {
    config := sarama.NewConfig()
    
    // Enable SASL authentication
    config.Net.SASL.Enable = true
    config.Net.SASL.User = cfg.SASLUsername
    config.Net.SASL.Password = cfg.SASLPassword
    config.Net.SASL.Handshake = true
    config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

    // Enable TLS for secure communication
    config.Net.TLS.Enable = true
    
    // Ensure the producer waits for message confirmations
    config.Producer.Return.Successes = true

    return config, nil
}
```

#### **CreateKafkaProducer Function**
```go:utils/utils.go
// CreateKafkaProducer initializes a Kafka SyncProducer with the given KafkaConfig.
// It establishes a connection to the Kafka cluster using the loaded configuration.
func CreateKafkaProducer(cfg KafkaConfig) (sarama.SyncProducer, error) {
    config, err := LoadKafkaConfig(cfg)
    if err != nil {
        return nil, fmt.Errorf("failed to load Kafka configuration: %v", err)
    }

    producer, err := sarama.NewSyncProducer([]string{cfg.BootstrapServers}, config)
    if err != nil {
        return nil, fmt.Errorf("failed to create Kafka producer: %v", err)
    }

    log.Println("Kafka producer initialized successfully")
    return producer, nil
}
```

---

## Testing

### Unit Tests
- **Kafka Topic Creation**:
  - Mock Kafka connection and test topic creation logic.
- **MongoDB Integration**:
  - Validate that MongoDB collections are correctly created and deleted.

---

## Dependencies

- **Kafka**: Uses IBM's Sarama client library for Go.
- **MongoDB**: Uses the official MongoDB Go Driver for database operations.
- **Environment Variables**:
  - `MONGO_URI`: MongoDB connection string.
  - `KAFKA_BOOTSTRAP_SERVERS`: Kafka cluster connection details.
  - `KAFKA_KEY`: SASL username for Kafka authentication.
  - `KAFKA_SECRET`: SASL password for Kafka authentication.

---

## Problems I Came Across and Handled 🧩

### Duplicate Producer Initialization

- **Problem:**
  - Initial implementation resulted in the Kafka producer being initialized twice, leading to resource inefficiency.

- **Solution:**
  - Refactored `main.go` to initialize the producer once and streamline topic creation alongside message sending.

### Import Path Mismatches

- **Problem:**
  - Used incorrect import paths (`github.com/Shopify/sarama` instead of `github.com/IBM/sarama`), causing dependency conflicts.

- **Solution:**
  - Updated all import statements to use the IBM-maintained Sarama library, ensuring consistency and compatibility.

### Environment Variable Loading

- **Problem:**
  - Faced issues with environment variables not being loaded correctly, resulting in authentication failures.

- **Solution:**
  - Verified environment variable setup and demonstrated how to set them securely in different operating systems.

### Kafka Topic Creation Permissions

- **Problem:**
  - Encountered permission errors when attempting to create Kafka topics programmatically, likely due to restricted API key permissions in Confluent Cloud.

- **Solution:**
  - Verified `go.mod` and import paths to ensure correct usage of the IBM Sarama library.
  - Attempted both implicit and explicit topic creation strategies to align with cluster configurations.
  - Coordinated with supervisor to gain necessary permissions for topic management.

---

## Next Steps 

### 1. Set Up Gin Server

- **Objective:**
  - Establish a Gin-based web server to handle HTTP requests for topic and segment management.

- **Tasks:**
  - Initialize the Gin router in `main.go`.
  - Create middleware for logging and error handling.

### 2. Implement API Endpoints

- **Endpoints to Develop:**
  - **Create/Delete Topic:**
    - Endpoint to handle JSON requests for creating or deleting Kafka topics.
  - **Create/Delete Segment:**
    - Endpoint to manage segments under specific topics based on incoming JSON schemas.

- **JSON Schema Handling:**
  - Define and validate incoming JSON structures to ensure data integrity.

### 3. Integrate MongoDB

- **Objective:**
  - Connect the aggregator to MongoDB to store metadata about topics and segments.

- **Tasks:**
  - Set up MongoDB client in `utils.go`.
  - Create functions to create and manage databases and collections corresponding to Kafka topics and segments.

### 4. Implement Segment Management Logic

- **Objective:**
  - Develop functionality to dynamically manage segments within topics, reflecting changes in both Kafka and MongoDB.

- **Tasks:**
  - Extend `aggregator.go` with segment-specific functions.
  - Ensure synchronization between Kafka topics and MongoDB collections.

### 5. Enhance API Security

- **Objective:**
  - Implement API key validation to secure endpoints and restrict access.

- **Tasks:**
  - Add middleware in the Gin server to validate incoming API keys.
  - Store valid API keys securely, possibly in environment variables or a configuration file.

### 6. Database Reflection of Changes

- **Objective:**
  - Ensure that any changes to topics or segments are accurately reflected in MongoDB.

- **Tasks:**
  - Develop event listeners or hooks that update MongoDB upon Kafka topic or segment modifications.

---

## Requirements to Proceed 📋

### Access Permissions

- **Confluent Cloud Dashboard:**
  - Gain access to manually create Kafka topics if automatic creation is restricted.
  - Verify and possibly upgrade API key permissions to allow topic and segment management.

### MongoDB Credentials

- **Database Access:**
  - Obtain MongoDB URI and necessary credentials to integrate with the aggregator.
  - Ensure network access between the aggregator service and MongoDB instance.

### API Key Management

- **Security Credentials:**
  - Define and distribute valid API keys for secure access to the Gin server endpoints.
  - Implement secure storage and rotation policies for API keys.

---

## Summary

The **Aggregation Module** has successfully established the foundational Kafka infrastructure, including secure connections and producer capabilities. Basic topic creation functionality has been implemented, laying the groundwork for more advanced features such as a Gin-based web server, API endpoints, and MongoDB integration. While challenges related to permissions and environment variable management were encountered and addressed, further collaboration with supervisory personnel is necessary to obtain required access permissions. The next phases will focus on building out the remaining components to achieve a fully functional and secure data aggregation system.

---

# **End of README**
