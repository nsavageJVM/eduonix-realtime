package com.eduonix.realtime;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ubu on 05.08.15.
 */
public class RealTimeEventProcessingTopology {

    private static final Logger LOG = Logger.getLogger(RealTimeEventProcessingTopology.class);


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        pipe_Kafka_To_Spout(topologyBuilder);

        pipe_Spout_To_Log_RealTimeEvents_Bolt(topologyBuilder);

        Config conf = new Config();
        conf.setDebug(true);

        StormSubmitter.submitTopology(GRID_CONFIG.TOPOLOGY_ID.getGridAttribute(),  conf, topologyBuilder.createTopology());


    }


    public static void pipe_Kafka_To_Spout(TopologyBuilder builder)
    {
        KafkaSpout kafkaSpout = new KafkaSpout(pipe_Kafka_To_Spout());

        builder.setSpout(GRID_CONFIG.KAFKA_SPOUT_ID.getGridAttribute(), kafkaSpout);
    }

    public static void pipe_Spout_To_Log_RealTimeEvents_Bolt(TopologyBuilder builder)
    {
        RealTimeEventsBolt logBolt = new RealTimeEventsBolt();
        builder.setBolt(GRID_CONFIG.LOG_RT_EVENT_BOLT_ID.getGridAttribute(), logBolt ).globalGrouping(GRID_CONFIG.KAFKA_SPOUT_ID.getGridAttribute());
    }


    private static SpoutConfig pipe_Kafka_To_Spout()
    {
        BrokerHosts hosts = new ZkHosts(GRID_CONFIG.KAFKA_ZOOKEEPER_HOST.getGridAttribute());
        String topic = GRID_CONFIG.KAFKA_TOPIC.getGridAttribute();
        String zkRoot = GRID_CONFIG.KAFKA_ZKROOT.getGridAttribute();
        String spoutId = GRID_CONFIG.SPOUT_ID.getGridAttribute();

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, spoutId);

        spoutConfig.scheme = new SchemeAsMultiScheme(new RealTimeEventScheme());

        return spoutConfig;
    }




    static class RealTimeEventsBolt extends BaseRichBolt {

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            //no output.
        }

        public void execute(Tuple tuple) {
            LOG.info(tuple.getStringByField(RealTimeEventScheme.LEGITIMATE_REAL_TIME)+ "," +
                    tuple.getStringByField(RealTimeEventScheme.LEGITIMATE_REAL_TIME_ID));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            //no output.
        }
    }


    static class  RealTimeEventScheme implements Scheme {

        public static final String LEGITIMATE_REAL_TIME  = "legitimate_event";

        public static final String LEGITIMATE_REAL_TIME_ID  = "legitimate_event_id";

        public List<Object> deserialize(byte[] bytes) {
            try
            {
            String real_time_Event = new String(bytes, "UTF-8");

                String[] data = real_time_Event.split("\t");
                String real_time = data[0];
                String real_time_Id = data[1];
                return new Values(real_time,real_time_Id);

            }
            catch (UnsupportedEncodingException e)
            {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }

        public Fields getOutputFields() {

            Fields keysForTuple = new Fields(LEGITIMATE_REAL_TIME, LEGITIMATE_REAL_TIME_ID);
            return keysForTuple;
        }
    }

}
