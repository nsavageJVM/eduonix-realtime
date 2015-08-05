# eduonix-realtime

/usr/hdp/2.2.4.2-2/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic realtime-event
/usr/hdp/2.2.4.2-2/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

java  ubu-p1.jar 

/usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic realtime-event --from-beginning

// run on cluster
storm jar ubu-sp1.jar  com.eduonix.realtime.RealTimeEventProcessingTopology

// run in local mode
mvn compile exec:java -Dstorm.topology=com.eduonix.realtime.RealTimeEventProcessingTopology


