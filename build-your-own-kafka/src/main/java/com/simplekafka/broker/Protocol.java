package com.simplekafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import com.simplekafka.broker.MethodReturnTypes.CreateTopicRequest;
import com.simplekafka.broker.MethodReturnTypes.FetchResponse;
import com.simplekafka.broker.MethodReturnTypes.FetchResult;
import com.simplekafka.broker.MethodReturnTypes.MetadataResult;
import com.simplekafka.broker.MethodReturnTypes.PartitionMetadata;
import com.simplekafka.broker.MethodReturnTypes.ProduceRequest;
import com.simplekafka.broker.MethodReturnTypes.ProduceResult;
import com.simplekafka.broker.MethodReturnTypes.TopicMetadata;

public class Protocol {
    // CLIENT REQUEST TYPES
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // BROKER RESPONSE TYPES
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Internal broker communication
    public static final byte REPLICATE = 0x21;
    public static final byte REPLICATE_ACK = 0x22;
    public static final byte TOPIC_NOTIFICATION = 0x23;
    


    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(11 + topic.length() + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes){
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        buffer.put(FETCH);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest(){
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int partitions, short replicationFactor){
        ByteBuffer buffer = ByteBuffer.allocate(9 + topic.length());
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(3 + errorMessage.length());
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorMessage.length());
        buffer.put(errorMessage.getBytes());
        buffer.flip();
        channel.write(buffer);
    }
    public static ByteBuffer encodeProduceResponse(long offset, String error){
        int size = error != null
            ? 1 + 2 + error.length()
            : 1 + 8 + 1;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        if (error != null) {
            buffer.put(ERROR_RESPONSE);
            buffer.putShort((short) error.length());
            buffer.put(error.getBytes());
        } else {
            buffer.put(PRODUCE_RESPONSE);
            buffer.putLong(offset);
            buffer.put((byte) 0); // success
        }
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message){
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length() + message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }
    public static ByteBuffer encodeTopicNotification(String topic){
        ByteBuffer buffer = ByteBuffer.allocate(3 + topic.length());
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.flip();
        return buffer;
    }
    public static ByteBuffer encodeFetchResponse(byte[][] messages, String error){
        int size = error != null
            ? 1 + 2 + error.length()
            : 1 + 4; // sadece tip + messageCount

        if (error == null) {
        for (byte[] msg : messages) {
            size += 4 + msg.length;
        }
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        if (error != null) {
            buffer.put(ERROR_RESPONSE);
            buffer.putShort((short) error.length());
            buffer.put(error.getBytes());
        } else {
            buffer.put(FETCH_RESPONSE);
            buffer.putInt(messages.length);
            for (byte[] msg : messages) {
                buffer.putInt(msg.length);
                buffer.put(msg);
            }
        }
        buffer.flip();
        return buffer;
    }
    public static ByteBuffer encodeMetadataResponse(List<BrokerInfo> brokers,List<TopicMetadata> topicMetadata){
        int size = 5 + 4;
        for (BrokerInfo broker : brokers){
            size += 4 + 2 + broker.getHost().length() + 4;
        }
        for (TopicMetadata topic : topicMetadata){
            size += 2 + topic.getTopic().length() + 4;
            for (PartitionMetadata partition : topic.getPartitions()){
                size += 4 + 4 + 4;
                size += partition.getReplicaIds().size() * 4;
            }
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(METADATA_RESPONSE);
        buffer.putInt(brokers.size());
        for (BrokerInfo broker : brokers){
            buffer.putInt(broker.getId());
            buffer.putShort((short) broker.getHost().length());
            buffer.put(broker.getHost().getBytes());
            buffer.putInt(broker.getPort());
        }
        buffer.putInt(topicMetadata.size());
        for (TopicMetadata topic : topicMetadata){
            buffer.putShort((short) topic.getTopic().length());
            buffer.put(topic.getTopic().getBytes());
            buffer.putInt(topic.getPartitions().size());
            for (PartitionMetadata partitionMetadata : topic.getPartitions()){
                buffer.putInt(partitionMetadata.getPartitionId());
                buffer.putInt(partitionMetadata.getLeaderId());
                buffer.putInt(partitionMetadata.getReplicaIds().size());
                for (int replicas : partitionMetadata.getReplicaIds()){
                    buffer.putInt(replicas);
                }
            }
        }
        buffer.flip();
        return buffer;
    }

    // DECODING RESPONSES
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer){
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }
        long offset = buffer.getLong();
        byte status = buffer.get();
        
        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }
    public static FetchResponse decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new FetchResponse(null, error);
            }
            return new FetchResponse(null, "Invalid response type");
        }
        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];
        for (int i = 0; i < messageCount; i++) {
            int messageLength = buffer.getInt();
            byte[] message = new byte[messageLength];
            buffer.get(message);
            messages[i] = message;
        }
        return new FetchResponse(messages, null);
    }


    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new MetadataResult(null, null, error);
            }
            return new MetadataResult(null, null, "Invalid response type");
        }
        List<BrokerInfo> brokers = new ArrayList<>();
        int brokerCount = buffer.getInt();
        for (int i = 0; i < brokerCount; i++) {
            int id = buffer.getInt();
            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes);
            int port = buffer.getInt();
            brokers.add(new BrokerInfo(id, host, port));
        }
        List<TopicMetadata> topics = new ArrayList<>();
        int topicCount = buffer.getInt();
        for (int i = 0; i < topicCount; i++) {
            short topicLength = buffer.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes);
            int partitionCount = buffer.getInt();

            List<PartitionMetadata> partitionMetadatas = new ArrayList<>();
            for (int k =0; k<partitionCount;k++){
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();
                int replicaCount = buffer.getInt();
                List<Integer> replicas = new ArrayList<>();
                for(int j=0;j<replicaCount;j++){
                    int replica = buffer.getInt();
                    replicas.add(replica);
                }
                PartitionMetadata metadata = new PartitionMetadata(partitionId, leaderId, replicas);
                partitionMetadatas.add(metadata);
            }
            TopicMetadata topic = new TopicMetadata(topicName, partitionMetadatas);
            topics.add(topic);
        }
        return new MetadataResult(brokers,topics,null);
    }


    // DECODING REQUESTS
    public static FetchResult decodeFetchRequest(ByteBuffer buffer){
        byte requestType = buffer.get();
        if (requestType != FETCH) {
            if (requestType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                return new FetchResult(null, -1, -1, -1);
            }
            return new FetchResult(null, -1, -1, -1);
        }
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();
        return new FetchResult(topic, partition, offset, maxBytes);
    
    }
    public static ProduceRequest decodeProduceRequest(ByteBuffer buffer){
        byte requestType = buffer.get();
        if (requestType != PRODUCE){
            if (requestType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                return new ProduceRequest(null, -1, null);
            }
            return new ProduceRequest(null, -1, null);
        }
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int partition = buffer.getInt();
        int messageLength = buffer.getInt();
        byte[] message = new byte[messageLength];
        buffer.get(message);
        return new ProduceRequest(topic, partition, message);
    }

    public static boolean decodeMetadataRequest(ByteBuffer buffer){
        byte requestType = buffer.get();
        return requestType == METADATA;
    }
    public static CreateTopicRequest decodeCreateTopicRequest(ByteBuffer buffer){
        byte requestType = buffer.get();
        if (requestType != CREATE_TOPIC){
            return null;
        }
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);
        int partitions = buffer.getInt();
        short replicationFactor = buffer.getShort();
        return new CreateTopicRequest(topic, partitions, replicationFactor);
    }

}

