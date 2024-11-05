from flask import Flask, jsonify
from kafka import KafkaConsumer
import json

app = Flask(__name__)

# Kafka configuration
kafka_broker = 'localhost:9092'
heartbeat_topic = 'heartbeat'
metrics_topic = 'metrics'

# Kafka consumers for metrics and heartbeat
metrics_consumer = KafkaConsumer(bootstrap_servers=kafka_broker,
                                 group_id='webserver_metrics_group',
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')))
metrics_consumer.subscribe([metrics_topic])  # Subscribe to the metrics topic

heartbeat_consumer = KafkaConsumer(bootstrap_servers=kafka_broker,
                                   group_id='webserver_heartbeat_group',
                                   value_deserializer=lambda x: json.loads(x.decode('utf-8')))
heartbeat_consumer.subscribe([heartbeat_topic])  # Subscribe to the heartbeat topic

# Initialize counters
request_counter = 0
response_counter = 0

@app.route('/ping')
def ping():
    global request_counter, response_counter
    return 'pong'

@app.route('/metrics')
def get_metrics():
    global request_counter, response_counter

    # Fetch the latest metrics message from Kafka using poll
    metrics_records = metrics_consumer.poll(100)  # adjust the timeout as needed
    metrics_message = next(iter(metrics_records.values()), None)
    
    if metrics_message:
        node_id = metrics_message[0].value.get('node_id', '')
        test_id = metrics_message[0].value.get('test_id', '')
        report_id = metrics_message[0].value.get('report_id', '')
        metrics_data = metrics_message[0].value.get('metrics', {})

        # Fetch the latest heartbeat from Kafka
        heartbeat_records = heartbeat_consumer.poll(100)  # adjust the timeout as needed
        heartbeat_message = next(iter(heartbeat_records.values()), None)
        heartbeat_timestamp = heartbeat_message[0].value.get('timestamp', '') if heartbeat_message else ''

        # Return metrics data and heartbeat as JSON response
        metrics_response = {
            'node_id': node_id,
            'test_id': test_id,
            'report_id': report_id,
            'metrics': metrics_data,
            #'heartbeat_timestamp': heartbeat_timestamp
        }

        return jsonify(metrics_response), 200

if __name__ == '__main__':
    app.run(debug=True)

