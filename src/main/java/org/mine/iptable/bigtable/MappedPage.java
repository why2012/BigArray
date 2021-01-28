package org.mine.iptable.bigtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MappedPage implements IMappedPage {
    private static Logger logger = LoggerFactory.getLogger(MappedPage.class);
    private MappedByteBuffer byteBuffer;
    private final int pageSizeInBytes;
    private volatile boolean closed;

    public MappedPage(MappedByteBuffer mappedByteBuffer, int pageSizeInBytes) {
        byteBuffer = mappedByteBuffer;
        this.pageSizeInBytes = pageSizeInBytes;
        closed = false;
    }

    @Override
    public int getInt(int index) {
        checkClosed();
        return byteBuffer.getInt(index);
    }

    @Override
    public void putInt(int index, int v) {
        checkClosed();
        byteBuffer.putInt(index, v);
    }

    @Override
    public void putInt(int v) {
        checkClosed();
        byteBuffer.putInt(v);
    }

    @Override
    public byte getByte(int index) {
        checkClosed();
        return byteBuffer.get(index);
    }

    @Override
    public void putByte(int index, byte v) {
        checkClosed();
        byteBuffer.put(index, v);
    }

    @Override
    public void putByte(byte v) {
        checkClosed();
        byteBuffer.put(v);
    }

    @Override
    public byte[] loadBytes(int offset, int length) {
        byte[] buf = new byte[length];
        for (int i = offset; i < length; i++) {
            buf[i] = byteBuffer.get(i);
        }
        return buf;
    }

    @Override
    public void putBytes(byte[] buf, int offset, int length) {
        byteBuffer.put(buf, offset, length);
        byteBuffer.force();
    }

    @Override
    public int[] load4Bytes(int offset, int length) {
        int[] buf = new int[length];
        for (int i = offset; i < length; i++) {
            buf[i] = byteBuffer.getInt(i * 4);
        }
        return buf;
    }

    @Override
    public void put4Bytes(int[] buf, int offset, int length) {
        for (int i = offset, bufIndex = 0; i < length; i++, bufIndex++) {
            byteBuffer.putInt(i * 4, buf[bufIndex]);
        }
    }

    @Override
    public void force() {
        byteBuffer.force();
    }

    private void checkClosed() {
        if (closed) {
            throw new RuntimeException("MappedPage already closed");
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        byteBuffer.force();
        Cleaner.clean(byteBuffer);
        byteBuffer = null;
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                try {
                    directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                } catch (Exception e) {
                    directBufferCleanerCleanX = Class.forName("jdk.internal.ref.Cleaner").getMethod("clean");
                }
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
                logger.error("Class load cleaner error", e);
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer byteBuffer) {
            if (byteBuffer == null) {
                return;
            }
            if (CLEAN_SUPPORTED && byteBuffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(byteBuffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    logger.error("clean error", e);
                }
            }
        }
    }
}
