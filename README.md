The project creates a cluster of Apache spark (Master - Worker) setup along with a Apache Kafka setup with Kraft consensus protocol. 

Apache kafka is used to manage the streams of data from 3 bioaggregators and Spark is used to run query over this stream of data. The final results of the spark query is saved in memory and published to kafka topic.

In order to start a stream from any of the available sources, list the sources which are currently active and access the results - we deploy a flask api server. 

Flask utilizes the python - subprocess package to create kafka topic, start the stream and submit jobs to spark cluster. 

The server is secured with a self-signed certificate with a basic (dummy) token-based auth setup. Port forwarding is enabled to forward the default HTTPS port on the server to the Flask app server port.


To setup the cluster and start the job you could follow below steps:
