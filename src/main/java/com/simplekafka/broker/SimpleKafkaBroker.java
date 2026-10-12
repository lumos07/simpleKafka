package com.simplekafka.broker;

import com.simplekafka.protocol.Protocol;
import com.simplekafka.protocol.Frame;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimpleKafkaBroker implements AutoCloseable {
    private static final int DEFAULT_PORT = 9092;

    private final int requestedPort;
    private final BrokerState state;
    private final ConsumerGroupCoordinator coordinator;
    private final ProducerStateManager producerState;
    private final ExecutorService clientExecutor;
    private final Map<Byte, RequestHandler> requestHandlers;

    private volatile boolean running;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    @FunctionalInterface
    public interface RequestHandler {
        ByteBuffer handle(ByteBuffer request);
    }

    /**
     * Primary constructor — all collaborators are injected.
     * Use {@link BrokerFactory} to create fully-wired instances.
     */
    SimpleKafkaBroker(int port, BrokerState state, ConsumerGroupCoordinator coordinator, ProducerStateManager producerState) {
        this.requestedPort = port;
        this.state = state;
        this.coordinator = coordinator;
        this.producerState = producerState;
        this.clientExecutor = Executors.newCachedThreadPool();
        this.requestHandlers = new HashMap<>();
        registerHandlers();
    }

    private void registerHandlers() {
        requestHandlers.put(Protocol.PRODUCE, req -> {
            Protocol.ProduceRequest produce = Protocol.decodeProduceRequest(req);
            if (produce.producerId >= 0) {
                ProducerStateManager.DeduplicationResult dedup =
                    producerState.checkSequence(
                        produce.producerId, produce.topic,
                        produce.partition, produce.sequenceNumber
                    );
                if (dedup instanceof ProducerStateManager.DeduplicationResult.Duplicate dup) {
                    return Protocol.encodeProduceResponse(dup.cachedOffset(), true);
                }
            }
            long offset = state.produce(produce.topic, produce.partition, produce.message);
            if (produce.producerId >= 0) {
                producerState.recordOffset(
                    produce.producerId, produce.topic,
                    produce.partition, produce.sequenceNumber, offset
                );
            }
            return Protocol.encodeProduceResponse(offset, false);
        });

        requestHandlers.put(Protocol.FETCH, req -> {
            Protocol.FetchRequest fetch = Protocol.decodeFetchRequest(req);
            List<byte[]> messages = state.fetch(
                fetch.topic, fetch.partition, fetch.offset, fetch.maxbytes
            );
            return Protocol.encodeFetchResponse(messages);
        });

        requestHandlers.put(Protocol.METADATA, req -> {
            Protocol.decodeMetadataRequest(req);
            return Protocol.encodeMetadataResponse(state.metadata().stream().map(md -> new Protocol.TopicMetaData(md.name(), md.partitionCount())).toList());
        });

        requestHandlers.put(Protocol.CREATE_TOPIC, req -> {
            Protocol.CreateTopicRequest create = Protocol.decodeCreateTopicRequest(req);
            state.createTopic(create.topic, create.numpartition);
            return Protocol.encodeCreateTopicResponse();
        });

        requestHandlers.put(Protocol.OFFSET_COMMIT, req -> {
            Protocol.OffsetCommitRequest commit = Protocol.decodeOffsetCommitRequest(req);
            state.commitOffset(
                commit.groupId, commit.topic, commit.partition, commit.offset
            );
            return Protocol.encodeOffsetCommitResponse();
        });

        requestHandlers.put(Protocol.OFFSET_FETCH, req -> {
            Protocol.OffsetFetchRequest offsetFetch = Protocol.decodeOffsetFetchRequest(req);
            long committedOffset = state.fetchCommittedOffset(
                offsetFetch.groupId, offsetFetch.topic, offsetFetch.partition
            );
            return Protocol.encodeOffsetFetchResponse(committedOffset);
        });

        requestHandlers.put(Protocol.JOIN_GROUP, req -> {
            Protocol.JoinGroupRequest joinReq = Protocol.decodeJoinGroupRequest(req);
            ConsumerGroupCoordinator.JoinResult joinResult = coordinator.join(
                joinReq.groupId, joinReq.memberId, joinReq.topic
            );
            return Protocol.encodeJoinGroupResponse(
                joinResult.memberId(), joinResult.generationId(), joinResult.assignments().stream().map(tp -> new Protocol.TopicPartition(tp.topic(), tp.partition())).toList()
            );
        });

        requestHandlers.put(Protocol.HEARTBEAT, req -> {
            Protocol.HeartbeatRequest hbReq = Protocol.decodeHeartbeatRequest(req);
            coordinator.heartbeat(hbReq.groupId, hbReq.memberId, hbReq.generationId);
            return Protocol.encodeHeartbeatResponse();
        });

        requestHandlers.put(Protocol.LEAVE_GROUP, req -> {
            Protocol.LeaveGroupRequest leaveReq = Protocol.decodeLeaveGroupRequest(req);
            coordinator.leave(leaveReq.groupId, leaveReq.memberId);
            return Protocol.encodeLeaveGroupResponse();
        });

        requestHandlers.put(Protocol.INIT_PRODUCER_ID, req -> {
            Protocol.decodeInitProducerIdRequest(req);
            long pid = producerState.allocateProducerId();
            return Protocol.encodeInitProducerIdResponse(pid);
        });
    }

    public synchronized void start() throws IOException {
        if (running) {
            return;
        }

        serverSocket = new ServerSocket(requestedPort);
        running = true;
        acceptThread = new Thread(this::acceptLoop, "simple-kafka-acceptor");
        acceptThread.start();
    }

    public int getPort() {
        ServerSocket socket = serverSocket;
        if (socket == null) {
            return requestedPort;
        }
        return socket.getLocalPort();
    }

    public ByteBuffer handleRequest(ByteBuffer request) {
        try {
            if (!request.hasRemaining()) {
                return Protocol.encodeErrorResponse("Empty request");
            }

            byte type = request.get();
            RequestHandler handler = requestHandlers.get(type);
            if (handler == null) {
                return Protocol.encodeErrorResponse("Unknown request type: " + type);
            }
            return handler.handle(request);
        } catch (RebalanceException e) {
            return Protocol.encodeErrorResponse(e.getMessage());
        } catch (OutOfOrderSequenceException e) {
            return Protocol.encodeErrorResponse(e.getMessage());
        } catch (IllegalArgumentException | IllegalStateException | IndexOutOfBoundsException e) {
            return Protocol.encodeErrorResponse(e.getMessage());
        }
    }

    public void awaitTermination() throws InterruptedException {
        Thread thread = acceptThread;
        if (thread != null) {
            thread.join();
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        IOException closeError = null;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                closeError = e;
            }
        }

        Thread thread = acceptThread;
        if (thread != null) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        clientExecutor.shutdownNow();
        try {
            clientExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        state.close();

        if (closeError != null) {
            throw closeError;
        }
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                clientExecutor.submit(() -> handleClient(client));
            } catch (SocketException e) {
                if (running) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                if (running) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleClient(Socket client) {
        try (Socket socket = client) {
            while (running && !socket.isClosed()) {
                ByteBuffer request;
                try {
                    request = Frame.read(socket.getInputStream());
                } catch (EOFException e) {
                    break;
                }

                ByteBuffer response = handleRequest(request);
                Frame.write(socket.getOutputStream(), response);
            }
        } catch (IOException e) {
            if (running) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length == 0 ? DEFAULT_PORT : Integer.parseInt(args[0]);
        Path dataDir = args.length < 2 ? Path.of("data") : Path.of(args[1]);
        SimpleKafkaBroker broker = BrokerFactory.create(port, dataDir);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                broker.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        broker.start();
        System.out.println("SimpleKafkaBroker listening on port " + broker.getPort());
        broker.awaitTermination();
    }
}
