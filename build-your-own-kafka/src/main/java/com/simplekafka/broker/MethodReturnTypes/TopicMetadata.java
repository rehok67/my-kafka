package com.simplekafka.broker.MethodReturnTypes;

import java.util.List;

public class TopicMetadata {
    private final String topic;
    private final List<PartitionMetadata> partitions;
    public TopicMetadata(String topic, List<PartitionMetadata> partitions) {
        this.topic = topic;
        this.partitions = partitions;
    }
    public String getTopic() {
        return topic;
    }
    public List<PartitionMetadata> getPartitions() {
        return partitions;
    }
}
