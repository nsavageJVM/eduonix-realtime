# eduonix-realtime data analytics tutorial code

## Local mode for testing and development  

`mvn compile exec:java -Dstorm.topology=com.eduonix.realtime.RealTimeEventProcessingTopology > test.txt`


## Build Kafka and Storm Artifacts with maven profiles
`mvn clean install -Pprod 
mvn clean install -Pspout
mv /root/eduonix-realtime/uberjar/*.jar /<staging directory>/`

## create Kafka stream topic ‘realtime-event’

`<path to>/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic realtime-event`

`<path to>/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181`


## run the Kafka producer class RealTimeEventProducer.java
`java -jar ubu-p1.jar`

`/usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic realtime-event --from-beginning`

## run on cluster
`storm jar ubu-sp1.jar  com.eduonix.realtime.RealTimeEventProcessingTopology`




