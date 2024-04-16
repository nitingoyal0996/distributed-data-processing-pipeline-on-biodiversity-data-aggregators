import sys

from kafka import KafkaConsumer
sys.path.append('/users/ngoyal')
import findspark
findspark.init()
import traceback
import subprocess
import json
from threading import Thread, Timer
from spark_submit import SparkJob
from flask import Flask, jsonify, request
# from confluent_kafka import Consumer, KafkaError
from src.kafka.consumer import MyConsumer
from src.kafka.topic import MyTopics
from src.kafka.runner.producer_main import stream_data_from_source
from urllib.parse import urlparse

app = Flask(__name__)

# meta data
kafka_brokers = ['128.110.217.192:9092','128.110.217.175:9092', '128.110.217.163:9092']

broker_source = {
                  "gbif": {
                    "address": "128.110.217.175:9092",
                    "spark_job_submitted": False,
                    "is_producer_online": False,
                    "source": "https://gbif.org",
                    "spark_job_object": None
                  },
                  "obis": {
                    "address": "128.110.217.192:9092",
                    "spark_job_submitted": False,
                    "is_producer_online": False,
                    "source": "https://obis.org",
                    "spark_job_object": None
                  },
                  "idigbio": {
                    "address" : "128.110.217.163:9092",
                    "spark_job_submitted": False,
                    "is_producer_online": False,
                    "source": "https://idigbio.org",
                    "spark_job_object": None
                  }
                }

def get_domain(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.split('.')[0]
    return domain

def get_messages(topic):
  def timeout_handler():
    print("Timeout expired. Killing process.")
    process.terminate()
  # Define the command to execute
  command = [
      "kafka-console-consumer.sh",
      "--bootstrap-server", "localhost:9092",
      "--topic", topic,
      "--from-beginning"
  ]
  # Open a subprocess to execute the command
  process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  # Create a Timer object to kill the process after 10 seconds
  timer = Timer(10, timeout_handler)
  timer.start()
  messages = []
  try:
    # Read the outputs while the process is running
    for output in process.stdout:
        message = output.decode('utf-8').strip()
        # Remove extra backslashes and quotes
        message = json.loads(message)
        messages.append(message)
  except:
    pass

  finally: 
    timer.cancel()
    process.terminate()
  # Check for errors
  if process.returncode != 0:
      # Handle errors
      print("Error:", process.stderr.read().decode('utf-8'))
  # Convert messages to JSON array
  json_array = json.dumps(messages)
  return json.loads(json_array)

def get_kingdom_count():
  pass

@app.route('/')
def hello_world():
  return jsonify({'message': 'Hello bio enthusiasts!'})

@app.route('/addSource')
def add_source():
  url = request.args.get('url') 
  if not url:
    return jsonify({'error': 'Missing required parameter "url"'}), 400 
  try:
    topic = get_domain(url)
    if topic in broker_source.keys():
      broker_address = broker_source.get(topic).get('address')
      # create a admin for topic
      kafka_admin = MyTopics(server=broker_address) 

      # create new topic
      kafka_admin.create_topic(topic)

      # Add producer to stream data on that broker
      def start_my_stream():
        produce_command = ["python", 
                 "/users/ngoyal/src/kafka/runner/producer_main.py", 
                 broker_address, 
                 topic]
        produce_process = subprocess.Popen(produce_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f'Submitted: {" ".join(produce_command)} \n Producer started streaming in the background. Continuing with the rest of the program..., {produce_process.pid}')
        
      Thread(target=start_my_stream).start()
      broker_source[topic]['is_producer_online'] = True

      def submit_my_job():
        spark_args = {
                        'master': 'spark://ms1132.utah.cloudlab.us:7077',
                        'name': 'spark_job_client',
                        'total_executor_cores': '8',
                        'executor_cores': '4',
                        'executor_memory': '4G',
                        'driver_memory': '2G',
                        'main_file_args': f'{topic}',
                        'packages': 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'
                      }

        main_file = '/users/ngoyal/src/spark/job_2.py'
        app = SparkJob(main_file, **spark_args)
        app.submit()
        
        # increment the spark_job_run_count key in the source object with one after completion and make the spark_job_running = False
        broker_source[topic]['spark_job_object'] = app

      Thread(target=submit_my_job).start()
      broker_source[topic]['spark_job_submitted'] = True
      
      return jsonify({"message": 'Successfully submitted the spark job'}), 200
  except Exception as e:
      print(traceback.format_exc())
      return jsonify({'error': 'Server failure'}), 500  
  else:
    return jsonify({'message': 'URL not found'}), 400

@app.route('/listSources')
def list_sources():
  # get list of topics from the kafka admin 
  # from the list of sources return the topics where the spark_job_submitted = True
  pass

@app.route('/count')
def count():
    by = request.args.get('by')
    if by not in ['kingdom', 'source', 'species']:
      return jsonify({"error": "Invalid filter value, allowed: ['kingdom', 'source', 'species']"}), 400
    
    # read the latest query results for all topics
    gbif_results = get_messages('gbif_query')[-1]
    obis_results = get_messages('obis_query')[-1]
    idigbio_results = get_messages('idigbio_query')[-1]
    print(f'gbif_results: f{gbif_results}')
    print(f'obis_results: f{obis_results}')
    print(f'idigbio_results: f{idigbio_results}')
  
    if by == 'kingdom':
      # consolidate the kingdom count
      pass
    elif by == 'source':
      # consolidate the source count 
      pass
    else:
      # consolidate the species count
      pass
    
    try:
      # messages = []
      return jsonify({"message": 'Message read successfully'}), 200
      # Handle timeout exception
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({'error': 'Server failure'}), 500

@app.errorhandler(404)
def handle_404():
  # Customize error message for clarity
  return jsonify({'error': 'not found'}), 404

if __name__ == '__main__':
  app.run(host='0.0.0.0', port=12700, debug=True)
