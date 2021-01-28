package org.mine.iptable.bigtable;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MappedPage implements IMappedPage {
    private static Logger logger = LoggerFactory.getLogger(MappedPage.class);
    private MappedByteBuffer byteBuffer;
    private volatile boolean closed;

    public MappedPage(MappedByteBuffer mappedByteBuffer) {
        byteBuffer = mappedByteBuffer;
        closed = false;
    }

    public int getInt(int index) {
        checkClosed();
        return byteBuffer.getInt(index);
    }

    public void putInt(int index, int v) {
        checkClosed();
        byteBuffer.putInt(index, v);
    }

    public void putInt(int v) {
        checkClosed();
        byteBuffer.putInt(v);
    }

    private void checkClosed() {
        if (closed) {
            throw new RuntimeException("MappedPage already closed");
        }
    }

    @Override
    public void close() throws Exception {
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
