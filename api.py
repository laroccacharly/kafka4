from flask import Flask, jsonify
import requests
import os

app = Flask(__name__)

PRODUCER_URL = "http://producer:5000"
CONSUMER_URL = "http://consumer:5000"

@app.route('/produce', methods=['POST'])
def trigger_produce(): 

    print(f"Triggering producer at {PRODUCER_URL}")
    try:
        response = requests.post(f"{PRODUCER_URL}")
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "error", "message": f"Failed to reach producer: {str(e)}"}), 500

@app.route('/consume', methods=['POST'])
def trigger_consume():
    print(f"Triggering consumer at {CONSUMER_URL}")
    try:
        response = requests.post(f"{CONSUMER_URL}")
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "error", "message": f"Failed to reach consumer: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
