from flask import Flask, jsonify, request
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
import sys
import time

app = Flask(__name__)

# Kafka configuration
kafka_broker = 'localhost:9092'
register_topic = 'register'
test_config_topic = 'test_config'
trigger_topic = 'trigger'
heartbeat_topic = 'heartbeat'
metrics_topic = 'metrics'

# In-memory storage for tracking registered nodes and test configurations
registered_nodes = {}
active_tests = {}

# Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=kafka_broker, group_id='orchestrator_group', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
consumer.subscribe([metrics_topic, heartbeat_topic])  # Subscribe to metrics and heartbeat topics

def send_kafka_message(topic, message):
    producer.send(topic, value=message)

@app.route('/start_test',methods=['POST'])
def start_test():
    data = request.get_json()
    test_id = str(uuid.uuid4())
    test_type = data['test_type']
    test_message_delay = data.get('test_message_delay', 0)
    message_count_per_driver = data['message_count_per_driver']

    test_config = {
        'test_id': test_id,
        'test_type': test_type,
        'test_message_delay': test_message_delay,
        'message_count_per_driver': message_count_per_driver
    }

    # Store test configuration for later reference
    active_tests[test_id] = test_config

    # Publish test configuration to all registered nodes
    send_kafka_message(test_config_topic, test_config)

    # Trigger the test
    trigger_message = {
        'test_id': test_id,
        'trigger': 'YES'
    }

    # Publish trigger message to all registered nodes
    send_kafka_message(trigger_topic, trigger_message)

@app.route('/get_metrics/<test_id>', methods=['GET'])
def get_metrics(test_id):
    # Placeholder for fetching and returning metrics
    # This should query the metrics store for the specified test_id
    # and return the relevant metrics data
    # Modify this according to your actual metrics storage implementation
    metrics = {
        'test_id': test_id,
        'metrics': 'Sample metrics data'
    }

    print(json.dumps(metrics))

def handle_heartbeat(heartbeat_message):
    # Handle heartbeat message from driver nodes
    node_id = heartbeat_message['node_id']
    timestamp = heartbeat_message['timestamp']
    # You can perform any necessary processing based on the heartbeat
    print(f"Heartbeat received from Node {node_id} at {timestamp}")

def handle_metrics(metrics_message):
    # Handle metrics message from driver nodes
    test_id = metrics_message['test_id']
    metrics_data = metrics_message['metrics']
    # You can store or process the metrics data as needed
    print(f"Metrics received for Test {test_id}: {metrics_data}")

def main():
    while True:
        # Consume messages from Kafka
        records = consumer.poll(100)  # adjust the timeout as needed

        for tp, consumer_records in records.items():
            for record in consumer_records:
                msg = record.value
                if tp.topic == heartbeat_topic:
                    handle_heartbeat(msg)
                elif tp.topic == metrics_topic:
                    handle_metrics(msg)


            #except json.JSONDecodeError as e:
             #   print(f"Error decoding JSON: {e}")

if __name__ == '__main__':
    # Start the Flask app
    app.run(port=5001, debug=True)
    main()
    
    # Run the Kafka consumer in parallel
