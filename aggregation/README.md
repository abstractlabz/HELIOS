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

## Examples
Examples in this documentation are included for complex or public-facing functions that require clarification on usage, involve external systems (external functions from APIs or other libararies), or have specific input/output requirements. Simple or self-explanatory utility functions do not require examples.

---

## Testing

### Unit Tests
- **Kafka Topic Creation**:
  - Mock Kafka connection and test topic creation logic.
- **MongoDB Integration**:
  - Validate that MongoDB collections are correctly created and deleted.

---

## Dependencies

- **Kafka**: Uses Confluent's Kafka client library for Go.
- **MongoDB**: Uses the official MongoDB Go Driver for database operations.
- **Environment Variables**:
  - `MONGO_URI`: MongoDB connection string.
  - `KAFKA_BOOTSTRAP_SERVERS`: Kafka cluster connection details.
