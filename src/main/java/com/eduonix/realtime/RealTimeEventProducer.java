package com.eduonix.realtime;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by ubu on 04.08.15.
 */
public class RealTimeEventProducer {

    private static final Logger LOG = Logger.getLogger(RealTimeEventProducer.class);

    private static final String brokerList= "sandbox.hortonworks.com:6667";
    private static final String zookeeperHost= "sandbox.hortonworks.com:2181";

    private static final String TOPIC = "realtime-event";

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("zk.connect", zookeeperHost);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        String event = "";
        int count = 0;

        while(count< 5000) {

            if(count%100==0) {

                event= "stream event illegitimate \t"+ count++;

            } else {

                event= "stream event legitimate \t "+ count++;
            }



            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, event);
            LOG.info("Sending Messge #:msg:" + event);
            producer.send(data);

        }



    }




}
