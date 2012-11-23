package org.neo4j.olap;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author mh
 * @since 23.11.12
 */
public class ArrayStore {
    static final int MEGABYTE = 1024 * 1024;
    private final String file;
    private final int capacity;

    public ArrayStore(String file, int capacity) {
        this.file = file;
        this.capacity = capacity;
    }

    public ArrayStore(String file) {
        this(file,MEGABYTE);
    }

    public int[] read() throws IOException {
        final FileInputStream is = new FileInputStream(file);
        long read = 0;
        int[] data;
        try {
            final FileChannel channel = is.getChannel();
            final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity * Integer.SIZE / 8);
            read = channel.read(buffer);
            buffer.limit((int) read);
            buffer.rewind();
            final int size = buffer.getInt();
            data = new int[size];
            for (int i = 0; i < size; i++) {
                if (buffer.position() == buffer.limit()) {
                    buffer.rewind();
                    final int readNow = channel.read(buffer);
                    if (readNow == -1) break;
                    read += readNow;
                    buffer.limit(readNow);
                    buffer.rewind();
                }
                data[i] = buffer.getInt();
            }
        } finally {
            is.close();
        }
        return data;
    }

    public long write(int[] data) throws IOException {
        final FileOutputStream os = new FileOutputStream(file);
        long written = 0;
        try {
            final FileChannel channel = os.getChannel();
            final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity * Integer.SIZE / 8);
            buffer.putInt(data.length);
            final int size = data.length;
            for (int i = 0; i < size; i++) {
                if (buffer.position() == buffer.limit()) {
                    buffer.rewind();
                    written += channel.write(buffer);
                    buffer.rewind();
                }
                buffer.putInt(data[i]);
            }
            buffer.limit(buffer.position());
            buffer.rewind();
            written += channel.write(buffer);
        } finally {
            os.close();
        }
        return written;
    }
}
