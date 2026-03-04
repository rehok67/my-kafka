package com.simplekafka.broker.MethodReturnTypes;

import java.util.List;

public class PartitionMetadata {
    private final int partitionId;
    private final int leaderId;
    private final List<Integer> replicaIds;
    public PartitionMetadata(int partitionId, int leaderId, List<Integer> replicaIds) {
        this.partitionId = partitionId;
        this.leaderId = leaderId;
        this.replicaIds = replicaIds;
    }
    public int getPartitionId() {
        return partitionId;
    }
    public int getLeaderId() {
        return leaderId;
    }
    public List<Integer> getReplicaIds() {
        return replicaIds;
    }
    
}
