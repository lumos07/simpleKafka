package com.simplekafka.protocol;

import com.simplekafka.protocol.Frame;

import org.junit.jupiter.api.Test;

// import java.beans.Transient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
// import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class FrameTest {
    
    @Test
    void testNormalRoundTrip()throws IOException{
        byte[] payload = "Hello Kafka".getBytes(StandardCharsets.UTF_8);
        ByteBuffer message = ByteBuffer.wrap(payload);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Frame.write(out, message);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        ByteBuffer result = Frame.read(in);

        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);

        assertArrayEquals(payload, resultBytes);
    }

    @Test
    void testSplitWrite()throws Exception{
        byte[] payload = "Hello Kafka this is longer message".getBytes(StandardCharsets.UTF_8);
        ByteBuffer message = ByteBuffer.wrap(payload);

        ByteArrayOutputStream frameBuf = new ByteArrayOutputStream();
        Frame.write(frameBuf, message);
        byte[] full_frame = frameBuf.toByteArray();

        PipedOutputStream piped_out = new PipedOutputStream();
        PipedInputStream piped_in = new PipedInputStream(piped_out);

        // Writer thread: trickles bytes in small chunks with delays,
        // simulating a slow/fragmented network arrival.

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(()->{
            try{
                int chunk_sz = 3;
                for(int i = 0;i < full_frame.length;i += chunk_sz){
                    int end = Math.min(i+chunk_sz,full_frame.length);
                    piped_out.write(full_frame, i,end-i);
                    piped_out.flush();
                    Thread.sleep(5);
                }piped_out.close();
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        });

        ByteBuffer result = Frame.read(piped_in);
        executor.shutdown();

        byte[] resultBytes = new byte[result.remaining()];
        result.get(resultBytes);

        assertArrayEquals(payload,resultBytes);
    }

    @Test
    void testTruncatedConnection()throws IOException{

        // Write a valid 4-byte length prefix claiming 100 bytes follow,
        // then close the stream after writing only 2 actual bytes.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        lenBuf.putInt(100);
        lenBuf.flip();

        out.write(lenBuf.array());
        out.write(new byte[]{1,2});

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        assertThrows(IOException.class, () -> Frame.read(in));
    }
}
