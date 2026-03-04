package com.simplekafka.broker.MethodReturnTypes;

import com.simplekafka.broker.BrokerInfo;
import java.util.List;

public class MetadataResult {
    private final List<BrokerInfo> brokers;
    private final List<TopicMetadata> topics;
    private final String error;
    public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
        this.brokers = brokers;
        this.topics = topics;
        this.error = error;
    }
    public List<BrokerInfo> getBrokers() {
        return brokers;
    }
    public List<TopicMetadata> getTopics() {
        return topics;
    }
    public String getError() {
        return error;
    }
    public boolean isSuccess() {
        return error == null;
    }
    
}
