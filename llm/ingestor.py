import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, List
from datetime import datetime

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from pinecone import Pinecone
from langchain.text_splitter import RecursiveCharacterTextSplitter

@dataclass
class KafkaMessage:
    topic: str
    segment: str
    ticker: str
    analysis: str
    raw_data: Any
    timestamp: int

@dataclass
class PineconeMetadata:
    id: str
    info: str
    current_date: int
    text: str
    ticker: str
    segment: str
    raw_data: Any

class WorkerPool:
    def __init__(self, num_workers: int, process_func):
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        self.process_func = process_func
        self.running = True

    def process_message(self, message):
        try:
            # Decode the Kafka message
            message_data = json.loads(message.value().decode('utf-8'))
            return self.process_func(message_data)
        except Exception as e:
            print(f"Error processing message: {e}")
            return None

    def start(self, consumer: Consumer):
        futures = []
        
        while self.running:
            try:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f"Consumer error: {message.error()}")
                    continue

                # Submit the message to the thread pool
                future = self.executor.submit(self.process_message, message)
                futures.append(future)

                # Clean up completed futures
                futures = [f for f in futures if not f.done()]
            except Exception as e:
                print(f"Error in worker pool: {e}")

    def stop(self):
        self.running = False
        self.executor.shutdown(wait=True)

class Ingestor:
    def __init__(self):
        load_dotenv()
        
        # Initialize Pinecone
        self.pinecone = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        self.index = self.pinecone.Index("main")
        
        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            'group.id': 'ingestor-group',
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv("KAFKA_KEY"),
            'sasl.password': os.getenv("KAFKA_SECRET")
        })
        
        self.consumer.subscribe(['alert_ingestor'])
        
        # Initialize text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=500,
            chunk_overlap=0
        )
        
        # Initialize worker pool
        self.worker_pool = WorkerPool(
            num_workers=5,
            process_func=self.process_message
        )

    def get_current_date(self) -> int:
        now = datetime.now()
        return now.year * 10000 + now.month * 100 + now.day

    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Pinecone's embedding service"""
        response = self.pinecone.embeddings.create(
            model_name="multilingual-e5-large",
            texts=texts,
            input_type="query",
            truncate="END"
        )
        return [embedding.values for embedding in response.embeddings]

    def process_message(self, message_data: dict) -> None:
        try:
            # Parse message
            message = KafkaMessage(**message_data)
            
            # Split text into chunks
            texts = self.text_splitter.split_text(message.analysis)
            
            # Process each chunk
            for text in texts:
                # Generate unique ID
                vector_id = str(uuid.uuid4())
                
                # Create metadata
                metadata = PineconeMetadata(
                    id=vector_id,
                    info=f"{message.ticker}-{message.segment}",
                    current_date=self.get_current_date(),
                    text=text,
                    ticker=message.ticker,
                    segment=message.segment,
                    raw_data=message.raw_data
                )
                
                # Generate embeddings
                embeddings = self.generate_embeddings([text])
                
                # Upsert to Pinecone
                self.index.upsert(
                    vectors=[{
                        "id": vector_id,
                        "values": embeddings[0],
                        "metadata": metadata.__dict__
                    }]
                )
                
                print(f"Successfully ingested data for ticker {message.ticker}, segment {message.segment}")
                
        except Exception as e:
            print(f"Error processing message: {e}")

    def start(self):
        try:
            print("Starting Ingestor...")
            self.worker_pool.start(self.consumer)
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            self.worker_pool.stop()
            self.consumer.close()

def main():
    ingestor = Ingestor()
    ingestor.start()

if __name__ == "__main__":
    main()