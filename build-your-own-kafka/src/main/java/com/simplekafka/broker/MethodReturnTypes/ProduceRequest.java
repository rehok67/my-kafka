package com.simplekafka.broker.MethodReturnTypes;

public class ProduceRequest {
    private final String topic;
    private final int partition;
    private final byte[] message;
    public ProduceRequest(String topic, int partition, byte[] message) {
        this.topic = topic;
        this.partition = partition;
        this.message = message;
    }
    public String getTopic() {
        return topic;
    }
    public int getPartition() {
        return partition;
    }
    public byte[] getMessage() {
        return message;
    }

}
