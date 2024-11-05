from flask import Flask, jsonify, request

app = Flask(name)

# File path for storing metrics
METRICS_FILE_PATH = 'metrics.json'

# Placeholder variables to simulate test state and results
load_test_running = False
test_results = {"mean_latency": 0, "error_rate": 0}

def write_metrics(metrics_data):
    """Write metrics to the JSON file."""
    with open(METRICS_FILE_PATH, 'w') as metrics_file:
        json.dump(metrics_data, metrics_file, indent=2)

def read_metrics():
    """Read metrics from the JSON file."""
    try:
        with open(METRICS_FILE_PATH, 'r') as metrics_file:
            return json.load(metrics_file)
    except FileNotFoundError:
        # Return an empty dictionary if the file doesn't exist yet
        return {}

# API endpoint to start the load test
@app.route('/start_test', methods=['POST'])
def start_test():
    global load_test_running
    if not load_test_running:
        # Perform logic to initialize the load test
        load_test_running = True
        return jsonify({"message": "Load test started successfully."})
    else:
        return jsonify({"message": "Load test is already running."})

# API endpoint to stop the load test
@app.route('/stop_test', methods=['POST'])
def stop_test():
    global load_test_running
    if load_test_running:
        # Perform logic to stop the load test
        load_test_running = False
        return jsonify({"message": "Load test stopped successfully."})
    else:
        return jsonify({"message": "No load test is currently running."})

# API endpoint to get test results
@app.route('/get_results', methods=['GET'])
def get_results():
    global test_results
    return jsonify(test_results)

# API endpoint to update metrics
@app.route('/update_metrics', methods=['POST'])
def update_metrics():
    # Get metrics data from the request
    metrics_data = request.json

    # Write metrics to the JSON file
    write_metrics(metrics_data)

    return jsonify({"message": "Metrics updated successfully."})

# API endpoint to get metrics
@app.route('/get_metrics', methods=['GET'])
def get_metrics():
    # Read metrics from the JSON file
    metrics = read_metrics()

    return jsonify(metrics)

if name == 'main':
    app.run(debug=True)
