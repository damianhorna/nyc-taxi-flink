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

To delete topics:
```
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic 'giorgos-.*'
```

Try out default producer/consumer:
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic testTopic

//and in another terminal:

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic testTopic --from-beginning
```

To purge Kafka topic you can first delete it and then recreate it:
```
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test

bin/kafka-topics.sh --create --zookeeper localhost:2181 \
    --replication-factor 1 --partitions 1 --topic test
```

### Run kafka producer
```
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
 com.example.bigdata.TestProducer data 15 testTopic \
 0 localhost:9092
```

or

```
java -cp /usr/local/kafka/libs/*:KafkaProducer.jar \
 com.example.bigdata.TestProducer data/yellow_tripdata_result 5 testTopic \
 0 localhost:9092
```