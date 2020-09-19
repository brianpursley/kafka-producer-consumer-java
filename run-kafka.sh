#!/bin/bash

# Download Kafka if needed
[ -d /tmp/kafka_2.13-2.6.0 ] || wget -qO - http://apache.mirrors.hoobly.com/kafka/2.6.0/kafka_2.13-2.6.0.tgz | tar xvfz - -C /tmp

cleanup() {
  echo "Cleaning up..."
  trap - INT
  # Delete topic
  /tmp/kafka_2.13-2.6.0/bin/kafka-topics.sh --delete --topic demo --bootstrap-server localhost:9092
  # Stop Broker
  kill -TERM $bpid && wait $bpid
  # Stop Zookeeper
  kill -TERM $zkpid && wait $zkpid
  exit 0
}

trap 'cleanup' INT

# Start Zookeeper
/tmp/kafka_2.13-2.6.0/bin/zookeeper-server-start.sh /tmp/kafka_2.13-2.6.0/config/zookeeper.properties &
zkpid=$!

# Start Broker
/tmp/kafka_2.13-2.6.0/bin/kafka-server-start.sh /tmp/kafka_2.13-2.6.0/config/server.properties &
bpid=$!

# Create topic
/tmp/kafka_2.13-2.6.0/bin/kafka-topics.sh --create --topic demo --partitions 8 --bootstrap-server localhost:9092

# Wait for Ctrl+C
read -r -d '' _ </dev/tty
