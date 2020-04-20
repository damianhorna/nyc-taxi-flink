#!/usr/bin/env bash

#producer
systemctl start zookeeper
systemctl start kafka

/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic testTopic
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testTopic
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar  com.example.bigdata.TestProducer ../nyc-taxi-flink/data/yellow_tripdata_result/ 5 testTopic  0 localhost:9092


#consumer
/home/dhorna/dev/tools/elasticsearch/elasticsearch-7.6.2/bin/elasticsearch
/home/dhorna/dev/tools/kibana/kibana-7.6.2-linux-x86_64/bin/kibana

curl -X DELETE 'http://localhost:9200/nyc-state'
curl -X DELETE 'http://localhost:9200/nyc-anomaly' 
curl -X PUT 'http://localhost:9200/nyc-state'
curl -X PUT 'http://localhost:9200/nyc-anomaly'

chromium 127.0.0.1:5601


#flink
java -jar flink-streaming-1.0-SNAPSHOT.jar --D 4 --L 5000


