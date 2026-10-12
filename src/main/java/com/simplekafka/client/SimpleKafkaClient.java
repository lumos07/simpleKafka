package com.simplekafka.client;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.simplekafka.protocol.Frame;
import com.simplekafka.protocol.Protocol;

public class SimpleKafkaClient implements AutoCloseable {
    private final Socket socket;

    public SimpleKafkaClient(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
    }

    public Protocol.CreateTopicResult createTopic(
        String topic,
        int partitionCount,
        short replicationFactor
    ) throws IOException {
        return Protocol.decodeCreateTopicResponse(
            send(Protocol.encodeCreateTopicRequest(topic, partitionCount, replicationFactor))
        );
    }

    public Protocol.InitProducerIdResult initProducerId() throws IOException {
        return Protocol.decodeInitProducerIdResponse(
            send(Protocol.encodeInitProducerIdRequest())
        );
    }

    public Protocol.ProduceResult produce(String topic, int partition, byte[] message)
        throws IOException {
        return Protocol.decodeProduceResponse(
            send(Protocol.encodeProducerequest(topic, partition, message))
        );
    }

    public Protocol.ProduceResult produce(
        long producerId, int sequenceNumber,
        String topic, int partition, byte[] message
    ) throws IOException {
        return Protocol.decodeProduceResponse(
            send(Protocol.encodeProducerequest(producerId, sequenceNumber, topic, partition, message))
        );
    }

    public Protocol.FetchResult fetch(String topic, int partition, long offset, int maxBytes)
        throws IOException {
        return Protocol.decodeFetchResponse(
            send(Protocol.encodeFetchrequest(topic, partition, offset, maxBytes))
        );
    }

    public Protocol.MetaDataResult metadata() throws IOException {
        return Protocol.decodeMetadataResponse(send(Protocol.encodeMetadatarequest()));
    }

    public Protocol.OffsetCommitResult commitOffset(
        String groupId,
        String topic,
        int partition,
        long offset
    ) throws IOException {
        return Protocol.decodeOffsetCommitResponse(
            send(Protocol.encodeOffsetCommitRequest(groupId, topic, partition, offset))
        );
    }

    public Protocol.OffsetFetchResult fetchCommittedOffset(
        String groupId,
        String topic,
        int partition
    ) throws IOException {
        return Protocol.decodeOffsetFetchResponse(
            send(Protocol.encodeOffsetFetchRequest(groupId, topic, partition))
        );
    }

    public Protocol.JoinGroupResult joinGroup(
        String groupId,
        String memberId,
        String topic
    ) throws IOException {
        return Protocol.decodeJoinGroupResponse(
            send(Protocol.encodeJoinGroupRequest(groupId, memberId, topic))
        );
    }

    public Protocol.HeartbeatResult heartbeat(
        String groupId,
        String memberId,
        int generationId
    ) throws IOException {
        return Protocol.decodeHeartbeatResponse(
            send(Protocol.encodeHeartbeatRequest(groupId, memberId, generationId))
        );
    }

    public Protocol.LeaveGroupResult leaveGroup(
        String groupId,
        String memberId
    ) throws IOException {
        return Protocol.decodeLeaveGroupResponse(
            send(Protocol.encodeLeaveGroupRequest(groupId, memberId))
        );
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    private synchronized ByteBuffer send(ByteBuffer request) throws IOException {
        Frame.write(socket.getOutputStream(), request);
        return Frame.read(socket.getInputStream());
    }
}
