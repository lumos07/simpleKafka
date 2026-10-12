package com.simplekafka.protocol;

import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Protocol {

    // client requests types 
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;
    public static final byte OFFSET_COMMIT = 0x05;
    public static final byte OFFSET_FETCH = 0x06;
    public static final byte JOIN_GROUP = 0x07;
    public static final byte HEARTBEAT = 0x08;
    public static final byte LEAVE_GROUP = 0x09;
    public static final byte INIT_PRODUCER_ID = 0x0A;

    // broker response types 
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte OFFSET_COMMIT_RESPONSE = 0x15;
    public static final byte OFFSET_FETCH_RESPONSE = 0x16;
    public static final byte JOIN_GROUP_RESPONSE = 0x17;
    public static final byte HEARTBEAT_RESPONSE = 0x18;
    public static final byte LEAVE_GROUP_RESPONSE = 0x19;
    public static final byte INIT_PRODUCER_ID_RESPONSE = 0x1A;
    public static final byte ERROR_RESPONSE = (byte)0x99;

    //!!! produce request !!!
    // --- encode --- 
    public static ByteBuffer encodeProducerequest(String topic,int partition,byte[] message){
        return encodeProducerequest(-1L, 0, topic, partition, message);
    }

    public static ByteBuffer encodeProducerequest(long producerId,int sequenceNumber,String topic,int partition,byte[] message){
        byte[] topicbytes = topic.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(topicbytes.length, "Topic");
        
        // request type + producerId + sequenceNumber + topic length + topic + partition id + message length + message 
        int capacity = 1 + 8 + 4 + 2 + topicbytes.length + 4 + 4 + message.length;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(PRODUCE);
        b.putLong(producerId);
        b.putInt(sequenceNumber);
        b.putShort((short)topicbytes.length);
        b.put(topicbytes);
        b.putInt(partition);
        b.putInt(message.length);
        b.put(message);

        b.flip();
        return b;
    }

    // --- decode ---
    public static ProduceRequest decodeProduceRequest(ByteBuffer b){
        // no need to check for the type
        // dispatcher has alredy consumed it i.e. checked it 
        long producerId = b.getLong();
        int sequenceNumber = b.getInt();
        String topic = extractString(b);
        int partition = b.getInt();
        int msglen = b.getInt();
        byte[] message = new byte[msglen];
        b.get(message);

        return new ProduceRequest(producerId, sequenceNumber, topic, partition, message);
    }

    // !!! fetch request !!!
    // --- encode ---
    public static ByteBuffer encodeFetchrequest(String topic,int partition,long offset,int maxbytes){
        byte[] topicbytes = topic.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(topicbytes.length, "Topic");

        int capacity = 1 + 2 + topicbytes.length + 4 + 8 + 4;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(FETCH);
        b.putShort((short)topicbytes.length);
        b.put(topicbytes);
        b.putInt(partition);
        b.putLong(offset);
        b.putInt(maxbytes);

        b.flip();
        return b;
    }

    // --- decode ---
    public static FetchRequest decodeFetchRequest(ByteBuffer b){
        String topic = extractString(b);
        int partition = b.getInt();
        long offset = b.getLong();
        int maxbytes = b.getInt();

        return new FetchRequest(topic, partition, offset, maxbytes);
    }

    // !!! metadata request !!!
    // --- encode ---
    public static ByteBuffer encodeMetadatarequest(){
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(METADATA);
        b.flip();
        return b;
    }

    // --- decode ---
    public static MetadataRequest decodeMetadataRequest(ByteBuffer b){
        return new MetadataRequest();
    }

    // !!! Create topic Request !!!
    // --- encode ---
    public static ByteBuffer encodeCreateTopicRequest(String topic,int numpartition,short replicationfactor){
        byte[] topicbytes = topic.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(topicbytes.length, "Topic");

        int capacity = 1 + 2 + topicbytes.length + 4 + 2;

        ByteBuffer b = ByteBuffer.allocate(capacity);

        b.put(CREATE_TOPIC);
        b.putShort((short)topicbytes.length);
        b.put(topicbytes);
        b.putInt(numpartition);
        b.putShort(replicationfactor);

        b.flip();
        return b;
    }

    // --- decode --- 
    public static CreateTopicRequest decodeCreateTopicRequest(ByteBuffer b){
        String topic = extractString(b);
        int numpartition = b.getInt();
        short replicationfactor = b.getShort();

        return new CreateTopicRequest(topic,numpartition,replicationfactor);
    }

    // !!! offset commit request !!!
    // --- encode ---
    public static ByteBuffer encodeOffsetCommitRequest(String groupId,String topic,int partition,long offset){
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(groupBytes.length, "Group");
        validateUnsignedShortLength(topicBytes.length, "Topic");

        int capacity = 1 + 2 + groupBytes.length + 2 + topicBytes.length + 4 + 8;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(OFFSET_COMMIT);
        putString(b, groupBytes);
        putString(b, topicBytes);
        b.putInt(partition);
        b.putLong(offset);
        b.flip();
        return b;
    }

    // --- decode ---
    public static OffsetCommitRequest decodeOffsetCommitRequest(ByteBuffer b){
        String groupId = extractString(b);
        String topic = extractString(b);
        int partition = b.getInt();
        long offset = b.getLong();

        return new OffsetCommitRequest(groupId,topic,partition,offset);
    }

    // !!! offset fetch request !!!
    // --- encode ---
    public static ByteBuffer encodeOffsetFetchRequest(String groupId,String topic,int partition){
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(groupBytes.length, "Group");
        validateUnsignedShortLength(topicBytes.length, "Topic");

        int capacity = 1 + 2 + groupBytes.length + 2 + topicBytes.length + 4;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(OFFSET_FETCH);
        putString(b, groupBytes);
        putString(b, topicBytes);
        b.putInt(partition);
        b.flip();
        return b;
    }

    // --- decode ---
    public static OffsetFetchRequest decodeOffsetFetchRequest(ByteBuffer b){
        String groupId = extractString(b);
        String topic = extractString(b);
        int partition = b.getInt();

        return new OffsetFetchRequest(groupId,topic,partition);
    }

    // !!! join group request !!!
    // --- encode ---
    public static ByteBuffer encodeJoinGroupRequest(String groupId,String memberId,String topic){
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] memberBytes = memberId.getBytes(StandardCharsets.UTF_8);
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(groupBytes.length, "Group");
        validateUnsignedShortLength(memberBytes.length, "Member");
        validateUnsignedShortLength(topicBytes.length, "Topic");

        int capacity = 1 + 2 + groupBytes.length + 2 + memberBytes.length + 2 + topicBytes.length;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(JOIN_GROUP);
        putString(b, groupBytes);
        putString(b, memberBytes);
        putString(b, topicBytes);
        b.flip();
        return b;
    }

    // --- decode ---
    public static JoinGroupRequest decodeJoinGroupRequest(ByteBuffer b){
        String groupId = extractString(b);
        String memberId = extractString(b);
        String topic = extractString(b);

        return new JoinGroupRequest(groupId,memberId,topic);
    }

    // !!! heartbeat request !!!
    // --- encode ---
    public static ByteBuffer encodeHeartbeatRequest(String groupId,String memberId,int generationId){
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] memberBytes = memberId.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(groupBytes.length, "Group");
        validateUnsignedShortLength(memberBytes.length, "Member");

        int capacity = 1 + 2 + groupBytes.length + 2 + memberBytes.length + 4;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(HEARTBEAT);
        putString(b, groupBytes);
        putString(b, memberBytes);
        b.putInt(generationId);
        b.flip();
        return b;
    }

    // --- decode ---
    public static HeartbeatRequest decodeHeartbeatRequest(ByteBuffer b){
        String groupId = extractString(b);
        String memberId = extractString(b);
        int generationId = b.getInt();

        return new HeartbeatRequest(groupId,memberId,generationId);
    }

    // !!! leave group request !!!
    // --- encode ---
    public static ByteBuffer encodeLeaveGroupRequest(String groupId,String memberId){
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);
        byte[] memberBytes = memberId.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(groupBytes.length, "Group");
        validateUnsignedShortLength(memberBytes.length, "Member");

        int capacity = 1 + 2 + groupBytes.length + 2 + memberBytes.length;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(LEAVE_GROUP);
        putString(b, groupBytes);
        putString(b, memberBytes);
        b.flip();
        return b;
    }

    // --- decode ---
    public static LeaveGroupRequest decodeLeaveGroupRequest(ByteBuffer b){
        String groupId = extractString(b);
        String memberId = extractString(b);

        return new LeaveGroupRequest(groupId,memberId);
    }

    // !!! produce response !!!
    //--- encode ---
    public static ByteBuffer encodeProduceResponse(long offset){
        return encodeProduceResponse(offset, false);
    }

    public static ByteBuffer encodeProduceResponse(long offset, boolean duplicate){
        ByteBuffer b = ByteBuffer.allocate(10);
        b.put(PRODUCE_RESPONSE);
        b.putLong(offset);
        b.put((byte)(duplicate ? 1 : 0));
        b.flip();

        return b;
    }

    // --- decode ---
    public static ProduceResult decodeProduceResponse(ByteBuffer b){
        byte type = b.get();

        if(type == ERROR_RESPONSE){
            return new ProduceResult(-1, false, extractString(b));
        }
        if(type != PRODUCE_RESPONSE){
            return new ProduceResult(-1, false, "Invalid response type!");
        }

        long offset = b.getLong();
        boolean duplicate = b.hasRemaining() && b.get() == 1;
        return new ProduceResult(offset, duplicate, null);
    }

    // !!! init producer id request !!!
    // --- encode ---
    public static ByteBuffer encodeInitProducerIdRequest(){
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(INIT_PRODUCER_ID);
        b.flip();
        return b;
    }

    // --- decode ---
    public static void decodeInitProducerIdRequest(ByteBuffer b){
        // no payload
    }

    // !!! init producer id response !!!
    // --- encode ---
    public static ByteBuffer encodeInitProducerIdResponse(long producerId){
        ByteBuffer b = ByteBuffer.allocate(9);
        b.put(INIT_PRODUCER_ID_RESPONSE);
        b.putLong(producerId);
        b.flip();
        return b;
    }

    // --- decode ---
    public static InitProducerIdResult decodeInitProducerIdResponse(ByteBuffer b){
        byte type = b.get();
        if(type == ERROR_RESPONSE){
            return new InitProducerIdResult(-1, extractString(b));
        }
        if(type != INIT_PRODUCER_ID_RESPONSE){
            return new InitProducerIdResult(-1, "Invalid response type!");
        }
        long producerId = b.getLong();
        return new InitProducerIdResult(producerId, null);
    }

    // !!! Fetch response !!! 
    // --- encode ---
    public static ByteBuffer encodeFetchResponse(List<byte[]> messages){
        int capacity = 1+4;// type + msgcnt

        for(int i = 0;i < messages.size();i++){
            byte[] msg = messages.get(i);
            capacity += 4+msg.length;
        }

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(FETCH_RESPONSE);
        b.putInt(messages.size());

        for(int i = 0;i < messages.size();i++){
            b.putInt(messages.get(i).length);
            b.put(messages.get(i));
        }
        b.flip();

        return b;
    }

    // --- decode ---
    public static FetchResult decodeFetchResponse(ByteBuffer b){
        byte type = b.get();

        if(type == ERROR_RESPONSE){
            return new FetchResult(null,extractString(b));
        }
        if(type != FETCH_RESPONSE){
            return new FetchResult(null,"Invalid response type!");
        }

        int messagecnt = b.getInt();
        List<byte[]> messages = new ArrayList<>(messagecnt);
        for(int i = 0;i < messagecnt;i++){
            // long msgoffset = b.getLong();
            int msglen = b.getInt();
            byte[] msgbytes = new byte[msglen];
            b.get(msgbytes);
            messages.add(msgbytes); 
        }

        return new FetchResult(messages,null);
    }

    // !!! Metadata Response !!!
    // --- encode ---
    public static ByteBuffer encodeMetadataResponse(List<TopicMetaData> topics){
        int capacity = 1 + 4; // type + topic count 

        for(int i = 0;i < topics.size();i++){
            byte[] nameBytes = topics.get(i).topic_name.getBytes(StandardCharsets.UTF_8);
            validateUnsignedShortLength(nameBytes.length, "Topic");
            capacity += 2 + nameBytes.length + 4;
        }

        ByteBuffer b = ByteBuffer.allocate(capacity);

        b.put(METADATA_RESPONSE);
        b.putInt(topics.size());
        for(int i = 0;i < topics.size();i++){
            byte[] nameBytes = topics.get(i).topic_name.getBytes(StandardCharsets.UTF_8);
            int len = nameBytes.length;
            int partitioncnt = topics.get(i).partition_count;
            b.putShort((short)len);
            b.put(nameBytes);
            b.putInt(partitioncnt);
        }
        b.flip();

        return b;
    }

    // --- decode ---
    public static MetaDataResult decodeMetadataResponse(ByteBuffer b){
        byte type = b.get();

        if(type == ERROR_RESPONSE){
            return new MetaDataResult(null,extractString(b));
        }
        if(type != METADATA_RESPONSE){
            return new MetaDataResult(null,"Invalid response type!");
        }

        int topicscnt = b.getInt();
        List<TopicMetaData> topics = new ArrayList<>();
        for(int i = 0;i < topicscnt;i++){
            int topic_len = Short.toUnsignedInt(b.getShort());
            byte[] t_name = new byte[topic_len];
            b.get(t_name);
            String name = new String(t_name,StandardCharsets.UTF_8);
            int partitioncnt = b.getInt();
            TopicMetaData topic = new TopicMetaData(name, partitioncnt);
            topics.add(topic);
        }

        return new MetaDataResult(topics, null);
    }

    // !!! create topic Response !!!
    // --- encode ---
    public static ByteBuffer encodeCreateTopicResponse(){
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(CREATE_TOPIC_RESPONSE);
        b.flip();

        return b;
    }

    // --- decode ---
    public static CreateTopicResult decodeCreateTopicResponse(ByteBuffer b){
        byte type = b.get();
        if(type == ERROR_RESPONSE){
            return new CreateTopicResult(extractString(b));
        }
        if(type != CREATE_TOPIC_RESPONSE){
            return new CreateTopicResult("Invalid response type!");
        }

        return new CreateTopicResult(null);
    } 

    // !!! offset commit response !!!
    // --- encode ---
    public static ByteBuffer encodeOffsetCommitResponse(){
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(OFFSET_COMMIT_RESPONSE);
        b.flip();

        return b;
    }

    // --- decode ---
    public static OffsetCommitResult decodeOffsetCommitResponse(ByteBuffer b){
        byte type = b.get();
        if(type == ERROR_RESPONSE){
            return new OffsetCommitResult(extractString(b));
        }
        if(type != OFFSET_COMMIT_RESPONSE){
            return new OffsetCommitResult("Invalid response type!");
        }

        return new OffsetCommitResult(null);
    }

    // !!! offset fetch response !!!
    // --- encode ---
    public static ByteBuffer encodeOffsetFetchResponse(long offset){
        ByteBuffer b = ByteBuffer.allocate(9);
        b.put(OFFSET_FETCH_RESPONSE);
        b.putLong(offset);
        b.flip();

        return b;
    }

    // --- decode ---
    public static OffsetFetchResult decodeOffsetFetchResponse(ByteBuffer b){
        byte type = b.get();
        if(type == ERROR_RESPONSE){
            return new OffsetFetchResult(-1,extractString(b));
        }
        if(type != OFFSET_FETCH_RESPONSE){
            return new OffsetFetchResult(-1,"Invalid response type!");
        }

        long offset = b.getLong();
        return new OffsetFetchResult(offset,null);
    }

    // !!! join group response !!!
    // --- encode ---
    public static ByteBuffer encodeJoinGroupResponse(String memberId,int generationId,List<TopicPartition> assignments){
        byte[] memberBytes = memberId.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(memberBytes.length, "Member");

        int capacity = 1 + 2 + memberBytes.length + 4 + 4;
        for(TopicPartition assignment : assignments){
            byte[] topicBytes = assignment.topic.getBytes(StandardCharsets.UTF_8);
            validateUnsignedShortLength(topicBytes.length, "Topic");
            capacity += 2 + topicBytes.length + 4;
        }

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(JOIN_GROUP_RESPONSE);
        putString(b, memberBytes);
        b.putInt(generationId);
        b.putInt(assignments.size());
        for(TopicPartition assignment : assignments){
            byte[] topicBytes = assignment.topic.getBytes(StandardCharsets.UTF_8);
            putString(b, topicBytes);
            b.putInt(assignment.partition);
        }
        b.flip();
        return b;
    }

    // --- decode ---
    public static JoinGroupResult decodeJoinGroupResponse(ByteBuffer b){
        byte type = b.get();
        if(type == ERROR_RESPONSE){
            return new JoinGroupResult(null,-1,null,extractString(b));
        }
        if(type != JOIN_GROUP_RESPONSE){
            return new JoinGroupResult(null,-1,null,"Invalid response type!");
        }

        String memberId = extractString(b);
        int generationId = b.getInt();
        int assignmentCnt = b.getInt();
        List<TopicPartition> assignments = new ArrayList<>(assignmentCnt);
        for(int i = 0;i < assignmentCnt;i++){
            String topic = extractString(b);
            int partition = b.getInt();
            assignments.add(new TopicPartition(topic,partition));
        }

        return new JoinGroupResult(memberId,generationId,assignments,null);
    }

    // !!! heartbeat response !!!
    // --- encode ---
    public static ByteBuffer encodeHeartbeatResponse(){
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(HEARTBEAT_RESPONSE);
        b.flip();

        return b;
    }

    // --- decode ---
    public static HeartbeatResult decodeHeartbeatResponse(ByteBuffer b){
        byte type = b.get();
        if(type == ERROR_RESPONSE){
            return new HeartbeatResult(extractString(b));
        }
        if(type != HEARTBEAT_RESPONSE){
            return new HeartbeatResult("Invalid response type!");
        }

        return new HeartbeatResult(null);
    }

    // !!! leave group response !!!
    // --- encode ---
    public static ByteBuffer encodeLeaveGroupResponse(){
        ByteBuffer b = ByteBuffer.allocate(1);
        b.put(LEAVE_GROUP_RESPONSE);
        b.flip();

        return b;
    }

    // --- decode ---
    public static LeaveGroupResult decodeLeaveGroupResponse(ByteBuffer b){
        byte type = b.get();
        if(type == ERROR_RESPONSE){
            return new LeaveGroupResult(extractString(b));
        }
        if(type != LEAVE_GROUP_RESPONSE){
            return new LeaveGroupResult("Invalid response type!");
        }

        return new LeaveGroupResult(null);
    }

    // Handling Error response 
    public static ByteBuffer encodeErrorResponse(String error){
        byte[] errBytes = error.getBytes(StandardCharsets.UTF_8);
        validateUnsignedShortLength(errBytes.length, "Error");
        int capacity = 1+2+errBytes.length;

        ByteBuffer b = ByteBuffer.allocate(capacity);
        b.put(ERROR_RESPONSE);
        b.putShort((short)errBytes.length);
        b.put(errBytes);
        b.flip();

        return b;
    }

    // *******************************************************************

    // Topic Metadata class 
    public static class TopicMetaData{
        public final String topic_name;
        public final int partition_count;

        public TopicMetaData(String topic_name,int partition_count){
            this.topic_name = topic_name;
            this.partition_count = partition_count;
        }
    }

    // Response and Request Classes 

    private static String extractString(ByteBuffer b){
        int len = Short.toUnsignedInt(b.getShort());
        byte[] Stringbytes = new byte[len];
        b.get(Stringbytes);
        return new String(Stringbytes,StandardCharsets.UTF_8);
    }

    private static void putString(ByteBuffer b, byte[] bytes) {
        b.putShort((short)bytes.length);
        b.put(bytes);
    }

    private static void validateUnsignedShortLength(int len, String field) {
        if (len > 0xFFFF) {
            throw new IllegalArgumentException(field + " is too long: " + len + " bytes");
        }
    }

    public static class ProduceRequest{
        public final long producerId;
        public final int sequenceNumber;
        public final String topic;
        public final int partition;
        public final byte[] message;

        public ProduceRequest(long producerId,int sequenceNumber,String topic,int partition,byte[] message){
            this.producerId = producerId;
            this.sequenceNumber = sequenceNumber;
            this.topic = topic;
            this.partition = partition;
            this.message = message;
        }
    }

    public static class FetchRequest{
        public final String topic;
        public final int partition;
        public final long offset;
        public final int maxbytes;

        public FetchRequest(String topic,int partition,long offset,int maxbytes){
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.maxbytes = maxbytes;
        }
    }

    public static class ProduceResult{
        public final long offset;
        public final boolean duplicate;
        public final String error;

        public ProduceResult(long offset,boolean duplicate,String error){
            this.offset = offset;
            this.duplicate = duplicate;
            this.error = error;
        }

        public boolean hasError(){return error != null;}
    }

    public static class InitProducerIdResult{
        public final long producerId;
        public final String error;

        public InitProducerIdResult(long producerId,String error){
            this.producerId = producerId;
            this.error = error;
        }

        public boolean hasError(){return error != null;}
    }

    public static class FetchResult{
        public final List<byte[]> messages;
        public final String error;
        
        public FetchResult(List<byte[]>msg,String e){
            this.messages = msg;
            this.error = e;
        }
        
        public boolean hasError(){return error != null;}
    }

    public static class MetadataRequest{}

    public static class OffsetCommitRequest{
        public final String groupId;
        public final String topic;
        public final int partition;
        public final long offset;

        public OffsetCommitRequest(String groupId,String topic,int partition,long offset){
            this.groupId = groupId;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }
    }

    public static class OffsetFetchRequest{
        public final String groupId;
        public final String topic;
        public final int partition;

        public OffsetFetchRequest(String groupId,String topic,int partition){
            this.groupId = groupId;
            this.topic = topic;
            this.partition = partition;
        }
    }

    public static class JoinGroupRequest{
        public final String groupId;
        public final String memberId;
        public final String topic;

        public JoinGroupRequest(String groupId,String memberId,String topic){
            this.groupId = groupId;
            this.memberId = memberId;
            this.topic = topic;
        }
    }

    public static class HeartbeatRequest{
        public final String groupId;
        public final String memberId;
        public final int generationId;

        public HeartbeatRequest(String groupId,String memberId,int generationId){
            this.groupId = groupId;
            this.memberId = memberId;
            this.generationId = generationId;
        }
    }

    public static class LeaveGroupRequest{
        public final String groupId;
        public final String memberId;

        public LeaveGroupRequest(String groupId,String memberId){
            this.groupId = groupId;
            this.memberId = memberId;
        }
    }

    public static class TopicPartition{
        public final String topic;
        public final int partition;

        public TopicPartition(String topic,int partition){
            this.topic = topic;
            this.partition = partition;
        }
    }

    public static class MetaDataResult{
        public final List<TopicMetaData> topics;
        public final String Error;

        public MetaDataResult(List<TopicMetaData> topics,String e){
            this.topics = topics;
            this.Error = e;
        }

        public boolean hasError(){return Error != null;}
    }

    public static class CreateTopicRequest{
        public final String topic;
        public final int numpartition;
        public final short replicationfactor;

        public CreateTopicRequest(String topic,int numpartition,short replicationfactor){
            this.topic = topic;
            this.numpartition = numpartition;
            this.replicationfactor = replicationfactor;
        }
    }

    public static class CreateTopicResult{
        public final String error;

        public CreateTopicResult(String e){
            this.error = e;
        }

        public boolean hasError(){return error != null;}
    }

    public static class OffsetCommitResult{
        public final String error;

        public OffsetCommitResult(String e){
            this.error = e;
        }

        public boolean hasError(){return error != null;}
    }

    public static class OffsetFetchResult{
        public final long offset;
        public final String error;

        public OffsetFetchResult(long offset,String e){
            this.offset = offset;
            this.error = e;
        }

        public boolean hasError(){return error != null;}
    }

    public static class JoinGroupResult{
        public final String memberId;
        public final int generationId;
        public final List<TopicPartition> assignments;
        public final String error;

        public JoinGroupResult(String memberId,int generationId,List<TopicPartition> assignments,String e){
            this.memberId = memberId;
            this.generationId = generationId;
            this.assignments = assignments;
            this.error = e;
        }

        public boolean hasError(){return error != null;}
    }

    public static class HeartbeatResult{
        public final String error;

        public HeartbeatResult(String e){
            this.error = e;
        }

        public boolean hasError(){return error != null;}
    }

    public static class LeaveGroupResult{
        public final String error;

        public LeaveGroupResult(String e){
            this.error = e;
        }

        public boolean hasError(){return error != null;}
    }
}
