package com.eduonix.realtime;

/**
 * Grid Configuration constants
 */
public enum GRID_CONFIG {


    ZOOKEEPER_HOST("sandbox.hortonworks.com:2181"),
    KAFKA_ZOOKEEPER_HOST("localhost"),
    KAFKA_TOPIC("realtime-event"),
    KAFKA_ZKROOT("/realtime-event_sprout"),
    KAFKA_BROKER_LIST("sandbox.hortonworks.com:6667"),
    KAFKA_SPOUT_ID("kafkaSpout"),
    SPOUT_ID("realtime-event"),
    LOG_RT_EVENT_BOLT_ID("logRealTimeEventBolt"),
    TOPOLOGY_ID("realtime-event-processor");

    private   String gridAttribute;

    GRID_CONFIG(String attribute) {
        this.gridAttribute = attribute;
    }

    public String getGridAttribute() {
        return gridAttribute;
    }
}
