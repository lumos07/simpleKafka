package com.simplekafka.storage;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

class PartitionLog {

    private final Path logFile;
    private final FileChannel appendChannel;
    private final List<Long> offsetPositions;
    private long nextOffset;
    private long currentPhysicalPosition;
    private int unflushedMessages;

    PartitionLog(Path logFile) {
        this.logFile = logFile;
        this.offsetPositions = buildIndex(logFile);
        this.nextOffset = this.offsetPositions.size();
        try {
            this.appendChannel = FileChannel.open(logFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            this.currentPhysicalPosition = this.appendChannel.size();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to open log file for appending: " + logFile, e);
        }
    }

    synchronized long append(byte[] message) {
        long offset = nextOffset;
        offsetPositions.add(currentPhysicalPosition);

        ByteBuffer header = ByteBuffer.allocate(Integer.BYTES);
        header.putInt(message.length);
        header.flip();
        ByteBuffer payload = ByteBuffer.wrap(message);

        try {
            writeFully(appendChannel, header);
            writeFully(appendChannel, payload);
            currentPhysicalPosition += Integer.BYTES + message.length;
            
            unflushedMessages++;
            int flushMessages = Integer.parseInt(System.getProperty("kafka.flush.messages", "1000"));
            if (unflushedMessages >= flushMessages) {
                flush();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to append to log: " + logFile, e);
        }

        nextOffset++;
        return offset;
    }

    synchronized void flush() {
        if (unflushedMessages > 0) {
            try {
                appendChannel.force(true);
                unflushedMessages = 0;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to flush log file: " + logFile, e);
            }
        }
    }

    synchronized void close() {
        flush();
        try {
            appendChannel.close();
        } catch (IOException e) {
            // Ignore during shutdown
        }
    }

    synchronized List<byte[]> read(long offset, int maxBytes) {
        if (offset >= nextOffset || maxBytes == 0) {
            return List.of();
        }

        List<byte[]> result = new ArrayList<>();
        int bytesRead = 0;
        
        // O(1) Index Lookup
        long physicalPosition = offsetPositions.get((int) offset);

        try (FileChannel readChannel = FileChannel.open(logFile, StandardOpenOption.READ)) {
            readChannel.position(physicalPosition);
            ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);

            while (true) {
                lengthBuffer.clear();
                int readLen = readChannel.read(lengthBuffer);
                if (readLen == -1) break;
                if (readLen < Integer.BYTES) {
                    throw new EOFException("Truncated record length in " + logFile);
                }
                
                lengthBuffer.flip();
                int messageLength = lengthBuffer.getInt();
                if (messageLength < 0) {
                    throw new IOException("Invalid message length: " + messageLength);
                }

                if (!result.isEmpty() && bytesRead + messageLength > maxBytes) {
                    break;
                }
                if (result.isEmpty() && messageLength > maxBytes) {
                    break;
                }

                ByteBuffer payloadBuffer = ByteBuffer.allocate(messageLength);
                readLen = readChannel.read(payloadBuffer);
                if (readLen < messageLength) {
                    throw new EOFException("Truncated record payload in " + logFile);
                }

                result.add(payloadBuffer.array());
                bytesRead += messageLength;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read log: " + logFile, e);
        }

        return result;
    }

    private static List<Long> buildIndex(Path logFile) {
        List<Long> index = new ArrayList<>();
        long currentPhysicalPosition = 0;
        
        try (InputStream in = Files.newInputStream(logFile)) {
            while (true) {
                byte[] lengthBytes = in.readNBytes(Integer.BYTES);
                if (lengthBytes.length == 0) {
                    return index;
                }
                if (lengthBytes.length < Integer.BYTES) {
                    throw new EOFException("Truncated record length in " + logFile);
                }
                
                index.add(currentPhysicalPosition);

                int messageLength = ByteBuffer.wrap(lengthBytes).getInt();
                if (messageLength < 0) {
                    throw new IOException("Invalid message length: " + messageLength);
                }

                skipFully(in, messageLength, logFile);
                currentPhysicalPosition += Integer.BYTES + messageLength;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load log: " + logFile, e);
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void skipFully(InputStream in, int bytes, Path logFile) throws IOException {
        int remaining = bytes;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped <= 0) {
                if (in.read() == -1) {
                    throw new EOFException("Truncated record payload in " + logFile);
                }
                skipped = 1;
            }
            remaining -= skipped;
        }
    }
}

