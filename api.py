import sys
sys.path.append('/users/ngoyal')
import traceback
import tempfile
import subprocess
from flask import Flask, jsonify, request
from src.kafka.topic import MyTopics
from src.kafka.runner.producer_main import stream_data_from_source
from urllib.parse import urlparse

app = Flask(__name__)

# meta data
kafka_brokers = ['128.110.217.192:9092','128.110.217.175:9094', '128.110.217.163:9093']

broker_source = {
                  "gbif": {
                    "address": "128.110.217.175:9092",
                    "spark_process": None,
                    "producer_process": None
                  },
                  "obis": {
                    "address": "128.110.217.192:9092",
                    "spark_process": None,
                    "producer_process": None
                  },
                  "idigbio": {
                    "address" : "128.110.217.163:9092",
                    "spark_process": None,
                    "producer_process": None
                  }
                }

def get_domain(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.split('.')[0]
    return domain

@app.route('/')
def hello_world():
  return jsonify({'message': 'Hello bio enthusiasts!'})

@app.route('/addSource')
def add_source():
  url = request.args.get('url')  # Get the URL parameter from the request
  if not url:
    return jsonify({'error': 'Missing required parameter "url"'}), 400  # Bad request for missing parameter
  try:
    topic = get_domain(url)
    if topic in broker_source.keys():
      broker_address = broker_source.get(topic).get('address')
      # create a admin for topic
      kafka_admin = MyTopics(server=broker_address) 

      # create new topic
      kafka_admin.create_topic(topic)
      
      # Add producer to stream data on that broker
      produce_command = ["python", 
                 "/users/ngoyal/src/kafka/runner/producer_main.py", 
                 broker_address, 
                 topic]
      produce_process = subprocess.Popen(produce_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      print(f'Submitted: {" ".join(produce_command)} \n Producer started streaming in the background. Continuing with the rest of the program..., {produce_process.pid}')
      
      broker_source[topic]["producer_process"] = produce_process

      # Submit to the spark job subscribed to this topic
      submit_command = ["spark-submit",
                "--num-executors", "4",
                "--executor-memory", "4G",
                "--driver-memory", "4G",
                "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1",
                "--master", "spark://ms1132.utah.cloudlab.us:7077",
                "/users/ngoyal/src/spark/job_2.py",
                topic]

      # Start the command in the background
      spark_process = subprocess.Popen(submit_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      print("Spark job started in the background. Continuing with the rest of the program...", spark_process.pid)
      
      broker_source[topic]["spark_process"] = spark_process

      # TODO: REMOVE THIS CODE
      # broker_source[topic]['producer_process'].terminate()
      # broker_source[topic]['spark_process'].terminate()
      # Test: Manually start the stream
      # stream_data_from_source(broker_address, topic)
      
      return jsonify({"message": 'Successfully submitted the spark job'}), 200  # URL found, return empty response with OK status
  except Exception as e:
      print(traceback.format_exc())
      return jsonify({'error': 'Server failure'}), 500  
  else:
    return jsonify({'message': 'URL not found'}), 400  # URL not found, return empty response with Bad request status


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=12700, debug=True)
