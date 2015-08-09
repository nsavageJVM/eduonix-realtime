package com.eduonix.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
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

    static boolean runOnCluster = true;


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        if(runOnCluster) {
            pipe_Kafka_To_Spout(topologyBuilder);
            pipe_Spout_To_Log_RealTimeEvents_Bolt(topologyBuilder);

            Config conf = new Config();
            conf.setDebug(true);

            pipe_Spout_To_Log_RealTimeEvents_Bolt(topologyBuilder);

            StormSubmitter.submitTopology(GRID_CONFIG.TOPOLOGY_ID.getGridAttribute(), conf, topologyBuilder.createTopology());

        } else {

            Config conf = new Config();
            conf.setDebug(true);
            conf.setNumWorkers(2);
            conf.setMaxTaskParallelism(3);


            topologyBuilder.setSpout("localSpout", new RealTimeEventsLocalSpout(), 5);
            topologyBuilder.setBolt(
                    "log", new RealTimeEventsLocalBolt(), 8)
                    .shuffleGrouping("localSpout")
                    .fieldsGrouping("localSpout", new Fields("realtime-event"));
            topologyBuilder.setBolt(
                    "test", new RealTimeEventsKafkaStreamBolt(), 8)
                    .fieldsGrouping("log",
                            new Fields(RealTimeEventScheme.LEGITIMATE_REAL_TIME,
                                    RealTimeEventScheme.LEGITIMATE_REAL_TIME_ID));


            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("test", conf, topologyBuilder.createTopology());

            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();

        }
    }


    public static void pipe_Kafka_To_Spout(TopologyBuilder builder) {
        KafkaSpout kafkaSpout = new KafkaSpout(pipe_Kafka_To_Spout());

        builder.setSpout(GRID_CONFIG.KAFKA_SPOUT_ID.getGridAttribute(), kafkaSpout);
    }


    public static void pipe_Spout_To_Log_RealTimeEvents_Bolt(TopologyBuilder builder)
    {
       // RealTimeEventsKafkaStreamBolt logBolt = new RealTimeEventsKafkaStreamBolt();
    //    builder.setBolt(GRID_CONFIG.LOG_RT_EVENT_BOLT_ID.getGridAttribute(), logBolt ).globalGrouping(GRID_CONFIG.KAFKA_SPOUT_ID.getGridAttribute());

        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");
        //Rotate every X minutes
        FileTimeRotationPolicy rotationPolicy = new FileTimeRotationPolicy
                (5, FileTimeRotationPolicy.Units.MINUTES);
        //Synchronize data buffer with the filesystem every 100 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(100);
        String fsUrl = "hdfs://sandbox.hortonworks.com:8020";

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/root" )
                .withPrefix("real_time_events");
        // Instantiate the HdfsBolt
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl(fsUrl)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        
        builder.setBolt(GRID_CONFIG.LOG_RT_EVENT_BOLT_ID.getGridAttribute(), hdfsBolt ).globalGrouping(GRID_CONFIG.KAFKA_SPOUT_ID.getGridAttribute());
    }


    private static SpoutConfig pipe_Kafka_To_Spout()
    {
        BrokerHosts hosts = new ZkHosts(GRID_CONFIG.KAFKA_ZOOKEEPER_HOST.getGridAttribute());
        String topic = GRID_CONFIG.KAFKA_TOPIC.getGridAttribute();
        String zkRoot = GRID_CONFIG.KAFKA_ZKROOT.getGridAttribute();
        String spoutId = GRID_CONFIG.KAFKA_SPOUT_ID.getGridAttribute();

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, spoutId);

        spoutConfig.scheme = new SchemeAsMultiScheme(new RealTimeEventScheme());

        return spoutConfig;
    }



    static class RealTimeEventsLocalBolt extends BaseBasicBolt {

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            //no output.
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {

            String real_time_Event =  tuple.getString(0);

            String[] data = real_time_Event.split("\t");
            String real_time = data[0];
            String real_time_Id = data[1];

            System.out.println("debugInfo: "+tuple);

            collector.emit(new Values(real_time,real_time_Id));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(
                    new Fields(RealTimeEventScheme.LEGITIMATE_REAL_TIME,  RealTimeEventScheme.LEGITIMATE_REAL_TIME_ID));
        }
    }

    static class RealTimeEventsKafkaStreamBolt extends BaseBasicBolt {

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            //no output.
        }

        public void execute(Tuple tuple, BasicOutputCollector collector) {

            String debugInfo = tuple.getStringByField(RealTimeEventScheme.LEGITIMATE_REAL_TIME )+ "," +
                    tuple.getStringByField(RealTimeEventScheme.LEGITIMATE_REAL_TIME_ID);


            System.out.println("RealTimeEventsKafkaStreamBolt local test debugInfo: "+tuple);

            collector.emit(new Values("RealTimeEventsKafkaStreamBolt  debugInfo:: "+tuple));
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
                return new Values(real_time, real_time_Id);

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

    static class RealTimeEventsLocalSpout implements IRichSpout {

        String event = "";
        int count = 0;

        SpoutOutputCollector _collector;

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("realtime-event"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            _collector = spoutOutputCollector;
        }

        public void close() {

        }

        public void activate() {

        }

        public void deactivate() {

        }

        public void nextTuple() {

            event= "stream event legitimate \t "+ count++;
            _collector.emit(new Values(event));
            Utils.sleep(100);

        }

        public void ack(Object o) {

        }

        public void fail(Object o) {

        }
    }



}
