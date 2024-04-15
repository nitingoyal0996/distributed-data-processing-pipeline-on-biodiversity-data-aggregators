<!-- https://superuser.com/questions/1317848/run-a-script-on-another-machine-accessible-through-another-machine-over-ssh -->
<!-- https://docs.confluent.io/kafka/operations-tools/kafka-tools.html -->
## Launch a new cluster ##
1. to launch the kafka cluster - we have to use the kraft server properties file. It exposes nodes for all of the listeners and control voters.

```conf
# ngoyal@node0:~$ cat /opt/kafka/config/kraft/server.properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093,2@128.110.217.163:9093,3@128.110.217.175:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://128.110.217.192:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/tmp/kraft-combined-logs
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
group.initial.rebalance.delay.ms=0
confluent.balancer.enable=true
confluent.license.topic.replication.factor=1
confluent.metadata.topic.replication.factor=1
confluent.balancer.topic.replication.factor=1
confluent.security.event.logger.exporter.kafka.topic.replicas=1
```

2. We create cluster id on one machine and provide it all other brokers trying to connect with that cluster. To make it eaiser use `launch-broker.sh` script. It will ask you to provide a cluster Id if the storage is not already formatted for any kafka cluster on that machine (aka if you are running kafka for the very first time.)

```bash
# generate cluster id on the first broker/controller
bash src/scripts/kafka/launch_broker.sh

# provide the cluster id to brokers
bash src/scripts/kafka/launch-broker.sh <cluster_id>
```

3. The ports should be available for kafka kraft to eastablish connection with the controllers and brokers. To check and make sure ports are available follow below steps - 

```bash
# for each broker - 
# list the java processes
jps -m

# run a test check whether the ports are listening to each other or not.
ping hostname -p port_number
# see if port is occupied by some process
netstat -anp | grep port_number
# check process name which is using the port
fuser port_number/tcp (or udp depending on the port type)
# free up the port
fuser -k port_number/tcp (or udp depending on the port type)
# if there is connection issue while running the cluster, check if port gets a hit when you run the cluster or not. If not there could be configuration issues.
nc -l port_number
```

4. We will run kafka in daemon mode, to check if the process has started run - `jps` in the terminal (output list should contain `Kafka`).

## Stopping Kafka ##

5. use `stop-broker.sh`


## Monitoring ##
To monitor kafka cluster an external kafka-ui package is running and could be accessible via port 2345

`http://node0.ngoyal-200000.ufl-eel6761-sp24-pg0.utah.cloudlab.us:2345`

