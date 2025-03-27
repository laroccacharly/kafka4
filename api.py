import logging
from flask import Flask, jsonify, request
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

PRODUCER_URL = "http://producer:5000"
CONSUMER_URL = "http://consumer:5000"

@app.route('/produce', methods=['POST'])
def trigger_produce(): 
    logger.info(f"Triggering producer at {PRODUCER_URL}")
    try:
        headers = {'Content-Type': 'application/json'}
        data = request.get_json()
        logger.info(f"Forwarding data: {data}")
        
        response = requests.post(f"{PRODUCER_URL}/", json=data, headers=headers)
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to reach producer: {str(e)}")
        return jsonify({"status": "error", "message": f"Failed to reach producer: {str(e)}"}), 500

@app.route('/consume', methods=['POST'])
def trigger_consume():
    logger.info(f"Triggering consumer at {CONSUMER_URL}")
    try:
        response = requests.post(f"{CONSUMER_URL}")
        return jsonify(response.json()), response.status_code
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to reach consumer: {str(e)}")
        return jsonify({"status": "error", "message": f"Failed to reach consumer: {str(e)}"}), 500

if __name__ == '__main__':
    logger.info("Starting API")
    app.run(host='0.0.0.0', port=5000)
