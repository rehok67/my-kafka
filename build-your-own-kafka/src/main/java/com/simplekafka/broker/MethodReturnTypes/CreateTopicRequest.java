package com.simplekafka.broker.MethodReturnTypes;

public class CreateTopicRequest {
    private final String topic;
    private final int numPartitions;
    private final short replicationFactor;
    public CreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        this.topic = topic;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
    }
    public String getTopic() {
        return topic;
    }
    public int getNumPartitions() {
        return numPartitions;
    }
    public int getReplicationFactor() {
        return replicationFactor;
    }
}
