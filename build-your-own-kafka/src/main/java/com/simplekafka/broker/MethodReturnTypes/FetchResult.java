package com.simplekafka.broker.ResultTypes;

public class FetchResult {
    private final String topic;
    private final int partition;
    private final long offset;
    private final int maxBytes;

    public FetchResult(String topic, int partition, long offset, int maxBytes) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.maxBytes = maxBytes;
    }
    public String getTopic() {
        return topic;
    }
    public int getPartition() {
        return partition;
    }
    public long getOffset() {
        return offset;
    }
    public int getMaxBytes() {
        return maxBytes;
    }

    
}
