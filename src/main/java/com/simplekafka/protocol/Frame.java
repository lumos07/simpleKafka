package com.simplekafka.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class Frame {
    private static final int MAX_FRAME_SIZE = 64 * 1024 * 1024;

    public static void write(OutputStream out,ByteBuffer message) throws IOException {
        int len = message.remaining();

        ByteBuffer prelen = ByteBuffer.allocate(4);
        prelen.putInt(len);
        prelen.flip();

        byte[] msgBytes = new byte[len];
        message.duplicate().get(msgBytes);

        out.write(prelen.array());
        out.write(msgBytes);
        out.flush();
    }

    public static ByteBuffer read(InputStream in)throws IOException{
        byte[] lenBytes = new byte[4];
        readFully(in, lenBytes);
        int len = ByteBuffer.wrap(lenBytes).getInt();
        if (len < 0 || len > MAX_FRAME_SIZE) {
            throw new IOException("Invalid frame length: " + len);
        }

        byte[] msgBytes = new byte[len];
        readFully(in, msgBytes);
        return ByteBuffer.wrap(msgBytes);
    }

    private static void readFully(InputStream in, byte[] buf) throws IOException{
        int total_read = 0;

        while(total_read < buf.length){
            int n = in.read(buf, total_read,buf.length-total_read);
            if(n < 0){
                throw new EOFException(
                    "Connection closed after " + total_read + " of " + buf.length + " bytes."
                );
            }
            total_read += n;
        }

    }
}
