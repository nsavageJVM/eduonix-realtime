package com.eduonix.realtime;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

/**
 * Created by ubu on 09.08.15.
 */
public class RealTimeEventsKafkaStreamToHDFSBolt extends BaseBasicBolt {

     static final String LEGITIMATE_REAL_TIME  = "legitimate_event";
     static final String LEGITIMATE_REAL_TIME_ID  = "legitimate_event_id";


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String dest = "hdfs://sandbox.hortonworks.com:8020/root/real_time_events.csv";
        Path dstPath = new Path(dest);
        FileSystem fileSystem = null;
        Configuration conf = new Configuration();
        String kafkaSpoutData = tuple.getStringByField(LEGITIMATE_REAL_TIME )+ "," + tuple.getStringByField(LEGITIMATE_REAL_TIME_ID);


        try {
             fileSystem = FileSystem.get(conf);

            if (!fileSystem.exists(dstPath)) {
                fileSystem.createNewFile(dstPath);
            }

            FSDataOutputStream fsout = fileSystem.append(dstPath);

            PrintWriter writer = new PrintWriter(fsout);
            writer.append(kafkaSpoutData);
            writer.close();

            fileSystem.close();

        } catch (IOException e) {
            e.printStackTrace();
        }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {



    }
}
