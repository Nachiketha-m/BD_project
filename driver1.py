import time
import json
import statistics
import uuid
import requests
from kafka import KafkaProducer, KafkaConsumer

# Kafka configuration
kafka_broker = 'localhost:9092'
requests_topic = 'requests'
responses_topic = 'responses'
heartbeat_topic = 'heartbeat'
metrics_topic = 'metrics'

# Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_broker,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=kafka_broker,
                         group_id='driver_node_group',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
consumer.subscribe([responses_topic])  # Subscribe to responses topic

# Driver node ID
node_id = str(uuid.uuid4())

def send_kafka_message(topic, message):
    producer.send(topic, value=message)

def send_request_to_webserver():
    # Simulate sending HTTP request to the target web server
    response_time = requests.get('http://localhost:5000/ping')
    response_time=response_time.elapsed.total_seconds()  # Replace this with your actual logic
    return response_time

def record_statistics(response_times):
    # Record statistics: mean, median, min, max
    mean_latency = statistics.mean(response_times)
    median_latency = statistics.median(response_times)
    min_latency = min(response_times)
    max_latency = max(response_times)
    return {
        "mean_latency": mean_latency,
        "median_latency": median_latency,
        "min_latency": min_latency,
        "max_latency": max_latency
    }

def send_results_to_orchestrator(results):
    # Send results to the Orchestrator via Kafka
    send_kafka_message(responses_topic, results)

def send_metrics(test_id, report_id, aggregated_stats):
    # Publish metrics to the metrics topic
    metrics_message = {
        "node_id": node_id,
        "test_id": test_id,
        "report_id": report_id,
        "metrics": aggregated_stats
    }
    send_kafka_message(metrics_topic, metrics_message)

def send_heartbeat():
    # Send heartbeat to the Orchestrator via Kafka
    heartbeat_message = {
        "node_id": node_id,
        "heartbeat": "YES",
        "timestamp": time.time()
    }
    send_kafka_message(heartbeat_topic, heartbeat_message)

def main():
    while True:
        # Simulate sending requests to the target web server
        response_times = []
        for i in range(5):  # You can adjust the number of requests per iteration
            start_time = time.time()
            response_time = send_request_to_webserver()
            response_times.append(response_time)
            end_time = time.time()

            # Record individual request statistics (if needed)
            individual_request_stats = {
                "request_number": i + 1,
                "response_time": response_time,
                "start_time": start_time,
                "end_time": end_time
            }
            print("Individual Request Stats:", individual_request_stats)

        # Record and send aggregated statistics to the Orchestrator
        aggregated_stats = record_statistics(response_times)

        # Generate random test_id and report_id
        test_id = str(uuid.uuid4())
        report_id = str(uuid.uuid4())

        # Publish metrics
        send_metrics(test_id, report_id, aggregated_stats)

        # Send heartbeat after processing each set of requests
        send_heartbeat()

        # Wait for a moment before the next iteration
        time.sleep(5)  # You can adjust the interval between iterations

if __name__ == '__main__':
    main()
