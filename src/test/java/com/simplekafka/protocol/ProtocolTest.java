package com.simplekafka.protocol;

import com.simplekafka.protocol.Protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

public class ProtocolTest {
    @Test
    void testProduceRequestRoundTrip(){
        String topic = "orders";
        int partition = 3;
        byte[] message = "Hello world".getBytes(StandardCharsets.UTF_8);

        ByteBuffer encoded = Protocol.encodeProducerequest(topic, partition, message);

        // Simulate the dispatcher: consume the type byte before decoding
        byte type = encoded.get();
        assertEquals(Protocol.PRODUCE,type);

        Protocol.ProduceRequest decoded = Protocol.decodeProduceRequest(encoded);
        assertEquals(topic, decoded.topic);
        assertEquals(partition, decoded.partition);
        assertArrayEquals(message, decoded.message);
    }

    @Test
    void testFetchRequestRoundTrip(){
        String topic = "payments";
        int partition = 1;
        long offset = 12345L;
        int maxbytes = 1024;

        ByteBuffer encoded = Protocol.encodeFetchrequest(topic, partition, offset, maxbytes);

        byte type = encoded.get();
        assertEquals(Protocol.FETCH, type);

        Protocol.FetchRequest decoded = Protocol.decodeFetchRequest(encoded);
        assertEquals(topic, decoded.topic);
        assertEquals(partition, decoded.partition);
        assertEquals(offset, decoded.offset);
        assertEquals(maxbytes, decoded.maxbytes);
    }

    @Test
    void testMetadataRequestRoundTrip(){
        ByteBuffer encoded = Protocol.encodeMetadatarequest();

        byte type = encoded.get();
        assertEquals(Protocol.METADATA,type);

        // Nothing left to decode — buffer should be fully drained
        assertFalse(encoded.hasRemaining());

        Protocol.MetadataRequest decoded = Protocol.decodeMetadataRequest(encoded);
        assertNotNull(decoded);
    }

    @Test
    void testCreateTopicRequestRoundTrip(){
        String topic = "analytics events";
        int numpartition = 6;
        short replicationfactor = 3;

        ByteBuffer encoded = Protocol.encodeCreateTopicRequest(topic, numpartition, replicationfactor);

        byte type = encoded.get();
        assertEquals(Protocol.CREATE_TOPIC, type);

        Protocol.CreateTopicRequest decoded = Protocol.decodeCreateTopicRequest(encoded);
        assertEquals(topic, decoded.topic);
        assertEquals(numpartition, decoded.numpartition);
        assertEquals(replicationfactor, decoded.replicationfactor);
    }

    @Test
    void testOffsetCommitRequestRoundTrip(){
        String groupId = "checkout-workers";
        String topic = "orders";
        int partition = 2;
        long offset = 42L;

        ByteBuffer encoded = Protocol.encodeOffsetCommitRequest(groupId, topic, partition, offset);

        byte type = encoded.get();
        assertEquals(Protocol.OFFSET_COMMIT, type);

        Protocol.OffsetCommitRequest decoded = Protocol.decodeOffsetCommitRequest(encoded);
        assertEquals(groupId, decoded.groupId);
        assertEquals(topic, decoded.topic);
        assertEquals(partition, decoded.partition);
        assertEquals(offset, decoded.offset);
    }

    @Test
    void testOffsetFetchRequestRoundTrip(){
        String groupId = "checkout-workers";
        String topic = "orders";
        int partition = 2;

        ByteBuffer encoded = Protocol.encodeOffsetFetchRequest(groupId, topic, partition);

        byte type = encoded.get();
        assertEquals(Protocol.OFFSET_FETCH, type);

        Protocol.OffsetFetchRequest decoded = Protocol.decodeOffsetFetchRequest(encoded);
        assertEquals(groupId, decoded.groupId);
        assertEquals(topic, decoded.topic);
        assertEquals(partition, decoded.partition);
    }

    @Test
    void testFetchResponseRoundTrip(){
        List<byte[]> msgs = new ArrayList<>();
        msgs.add(("Hi this is first message").getBytes(StandardCharsets.UTF_8));
        msgs.add(("Hi this is second message").getBytes(StandardCharsets.UTF_8));
        msgs.add(("Hi this is third meaage").getBytes(StandardCharsets.UTF_8));

        ByteBuffer encoded = Protocol.encodeFetchResponse(msgs);
        Protocol.FetchResult decoded = Protocol.decodeFetchResponse(encoded);
        
        // assertEquals(Protocol.FETCH_RESPONSE, encoded.get());
        // no need here because as unlike request we are checking the type in deocde func

        assertFalse(decoded.hasError());
        assertEquals(msgs.size(), decoded.messages.size());
        for(int i = 0;i < decoded.messages.size();i++){
            assertArrayEquals(msgs.get(i),decoded.messages.get(i));
        }
    }

    @Test
    void testEmptyFetchResponseRoundTrip(){
        List<byte[]> msgs = new ArrayList<>();

        ByteBuffer encoded = Protocol.encodeFetchResponse(msgs);
        Protocol.FetchResult decoded = Protocol.decodeFetchResponse(encoded);

        assertFalse(decoded.hasError());
        assertEquals(msgs.size(), decoded.messages.size());
        for(int i = 0;i < decoded.messages.size();i++){
            assertArrayEquals(msgs.get(i), decoded.messages.get(i));
        }
    }

    @Test
    void testCreateTopicResponseRoundTrip(){
        ByteBuffer encoded = Protocol.encodeCreateTopicResponse();
        Protocol.CreateTopicResult decoded = Protocol.decodeCreateTopicResponse(encoded);

        assertFalse(decoded.hasError());
    }

    @Test
    void testOffsetCommitResponseRoundTrip(){
        ByteBuffer encoded = Protocol.encodeOffsetCommitResponse();
        Protocol.OffsetCommitResult decoded = Protocol.decodeOffsetCommitResponse(encoded);

        assertFalse(decoded.hasError());
    }

    @Test
    void testOffsetFetchResponseRoundTrip(){
        ByteBuffer encoded = Protocol.encodeOffsetFetchResponse(77L);
        Protocol.OffsetFetchResult decoded = Protocol.decodeOffsetFetchResponse(encoded);

        assertFalse(decoded.hasError());
        assertEquals(77L, decoded.offset);
    }

    @Test
    void testMetaDataResponseRoundTrip(){
        List<Protocol.TopicMetaData> topics = new ArrayList<>();

        Protocol.TopicMetaData first = new Protocol.TopicMetaData("café", 2);
        Protocol.TopicMetaData second = new Protocol.TopicMetaData("topïc", 1);

        topics.add(first);
        topics.add(second);

        ByteBuffer encoded = Protocol.encodeMetadataResponse(topics);
        Protocol.MetaDataResult decoded = Protocol.decodeMetadataResponse(encoded);

        assertFalse(decoded.hasError());

        int sz = decoded.topics.size();
        assertEquals(topics.size(),sz);

        for(int i = 0;i < sz;i++){
            assertEquals(topics.get(i).topic_name,decoded.topics.get(i).topic_name);
            assertEquals(topics.get(i).partition_count, decoded.topics.get(i).partition_count);
        }
    }

    @Test
    void testJoinGroupResponseRoundTrip(){
        String memberId = "member-abc-123";
        int generationId = 7;
        List<Protocol.TopicPartition> assignments = new ArrayList<>();
        assignments.add(new Protocol.TopicPartition("orders", 0));
        assignments.add(new Protocol.TopicPartition("orders", 2));
        assignments.add(new Protocol.TopicPartition("orders", 4));

        ByteBuffer encoded = Protocol.encodeJoinGroupResponse(memberId, generationId, assignments);
        Protocol.JoinGroupResult decoded = Protocol.decodeJoinGroupResponse(encoded);

        assertFalse(decoded.hasError());
        assertEquals(memberId, decoded.memberId);
        assertEquals(generationId, decoded.generationId);
        assertEquals(3, decoded.assignments.size());
        assertEquals("orders", decoded.assignments.get(0).topic);
        assertEquals(0, decoded.assignments.get(0).partition);
        assertEquals(2, decoded.assignments.get(1).partition);
        assertEquals(4, decoded.assignments.get(2).partition);
    }

    @Test
    void testHeartbeatRequestRoundTrip(){
        String groupId = "checkout-workers";
        String memberId = "member-xyz";
        int generationId = 3;

        ByteBuffer encoded = Protocol.encodeHeartbeatRequest(groupId, memberId, generationId);

        byte type = encoded.get();
        assertEquals(Protocol.HEARTBEAT, type);

        Protocol.HeartbeatRequest decoded = Protocol.decodeHeartbeatRequest(encoded);
        assertEquals(groupId, decoded.groupId);
        assertEquals(memberId, decoded.memberId);
        assertEquals(generationId, decoded.generationId);
    }

    @Test
    void testHeartbeatResponseRoundTrip(){
        ByteBuffer encoded = Protocol.encodeHeartbeatResponse();
        Protocol.HeartbeatResult decoded = Protocol.decodeHeartbeatResponse(encoded);

        assertFalse(decoded.hasError());
    }

    @Test
    void testLeaveGroupRequestRoundTrip(){
        String groupId = "checkout-workers";
        String memberId = "member-xyz";

        ByteBuffer encoded = Protocol.encodeLeaveGroupRequest(groupId, memberId);

        byte type = encoded.get();
        assertEquals(Protocol.LEAVE_GROUP, type);

        Protocol.LeaveGroupRequest decoded = Protocol.decodeLeaveGroupRequest(encoded);
        assertEquals(groupId, decoded.groupId);
        assertEquals(memberId, decoded.memberId);
    }

    @Test
    void testLeaveGroupResponseRoundTrip(){
        ByteBuffer encoded = Protocol.encodeLeaveGroupResponse();
        Protocol.LeaveGroupResult decoded = Protocol.decodeLeaveGroupResponse(encoded);

        assertFalse(decoded.hasError());
    }

    @Test
    void testJoinGroupRequestRoundTrip(){
        String groupId = "checkout-workers";
        String memberId = "member-xyz";
        String topic = "orders";

        ByteBuffer encoded = Protocol.encodeJoinGroupRequest(groupId, memberId, topic);

        byte type = encoded.get();
        assertEquals(Protocol.JOIN_GROUP, type);

        Protocol.JoinGroupRequest decoded = Protocol.decodeJoinGroupRequest(encoded);
        assertEquals(groupId, decoded.groupId);
        assertEquals(memberId, decoded.memberId);
        assertEquals(topic, decoded.topic);
    }
}
