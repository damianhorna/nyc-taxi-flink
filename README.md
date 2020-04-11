# nyc-taxi-flink

1. Download data:
```
cd data
wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/taxi_zone_lookup.csv
wget http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/yellow_tripdata_result.zip
unzip yellow_tripdata_result.zip
```
 
 
### Kafka setup
To install kafka: https://tecadmin.net/install-apache-kafka-ubuntu/

You can also use docker.

Start zookeeper and kafka:
```
sudo systemctl start zookeeper
sudo systemctl start kafka
```

To create a topic:
```
cd /usr/local/kafka
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testTopic
```

To list topics:
```
bin/kafka-topics.sh --list --zookeeper localhost:2181
```

Try out default producer/consumer:
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic testTopic

//and in another terminal:

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic testTopic --from-beginning
```
