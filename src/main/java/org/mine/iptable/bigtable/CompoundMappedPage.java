package org.mine.iptable.bigtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class CompoundMappedPage implements IMappedPage {
    private static Logger logger = LoggerFactory.getLogger(CompoundMappedPage.class);
    private final LRUCache<Integer, IMappedPage> pageCache;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final int pageSizeInBytes;
    private final int subPageSizeInBytes;
    private final int maxSubPage;
    private volatile boolean closed = false;
    private volatile int pageCount = 0;

    public CompoundMappedPage(RandomAccessFile randomAccessFile, int pageSizeInBytes, int subPageSizeInBytes, int maxSubPage, int maxSubPageInMem) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = randomAccessFile.getChannel();
        this.pageSizeInBytes = pageSizeInBytes;
        this.subPageSizeInBytes = subPageSizeInBytes;
        this.maxSubPage = maxSubPage;
        pageCache = new LRUCache<>(maxSubPageInMem);
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        pageCache.expireAll();
        fileChannel.close();
        randomAccessFile.close();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private void checkClosed() {
        if (closed) {
            throw new RuntimeException("MappedPage already closed");
        }
    }

    private void checkPageCount(int subPageIndex) {
        if (subPageIndex + 1 > maxSubPage) {
            throw new IndexOutOfBoundsException("maxSubPage " + maxSubPage + ", already used " + pageCount);
        }
    }

    @Override
    public int getInt(int index) {
        checkClosed();
        return loadPage(getSubPageIndex(index)).getInt(getIndexInSubPage(index));
    }

    @Override
    public void putInt(int index, int v) {
        checkClosed();
        loadPage(getSubPageIndex(index)).putInt(getIndexInSubPage(index), v);
    }

    @Override
    public void putInt(int v) {
        checkClosed();
        if (pageCount == 0) {
            loadPage(0).putInt(v);
        } else {
            loadPage(pageCount - 1).putInt(v);
        }
    }

    @Override
    public byte getByte(int index) {
        checkClosed();
        return loadPage(getSubPageIndex(index)).getByte(getIndexInSubPage(index));
    }

    @Override
    public void putByte(int index, byte v) {
        checkClosed();
        loadPage(getSubPageIndex(index)).putByte(getIndexInSubPage(index), v);
    }

    @Override
    public void putByte(byte v) {
        checkClosed();
        if (pageCount == 0) {
            loadPage(0).putByte(v);
        } else {
            loadPage(pageCount - 1).putByte(v);
        }
    }

    @Override
    public byte[] loadBytes(int offset, int length) {
        byte[] buf = null;
        try {
            buf = new byte[length];
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, pageSizeInBytes);
            mappedByteBuffer.get(buf, offset, length);
            MappedPage.Cleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
        } catch (Exception e) {
            logger.error("load bytes", e);
        }
        return buf;
    }

    @Override
    public void putBytes(byte[] buf, int offset, int length) {
        try {
            pageCache.expireAll();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, pageSizeInBytes);
            mappedByteBuffer.put(buf, offset, length);
            mappedByteBuffer.force();
            MappedPage.Cleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
        } catch (Exception e) {
            logger.error("load bytes", e);
        }
    }

    @Override
    public int[] load4Bytes(int offset, int length) {
        int[] buf = null;
        try {
            buf = new int[length];
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, pageSizeInBytes);
            for (int i = offset; i < length; i++) {
                buf[i] = mappedByteBuffer.getInt(i * 4);
            }
            MappedPage.Cleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
        } catch (Exception e) {
            logger.error("load bytes", e);
        }
        return buf;
    }

    @Override
    public void put4Bytes(int[] buf, int offset, int length) {
        try {
            pageCache.expireAll();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, pageSizeInBytes);
            for (int i = offset, bufIndex = 0; i < length; i++, bufIndex++) {
                mappedByteBuffer.putInt(i * 4, buf[bufIndex]);
            }
            mappedByteBuffer.force();
            MappedPage.Cleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
        } catch (Exception e) {
            logger.error("load bytes", e);
        }
    }

    @Override
    public void force() {
        for (IMappedPage mappedPage: pageCache.values()) {
            mappedPage.force();
        }
    }

    private int getSubPageIndex(int index) {
        return index / subPageSizeInBytes;
    }

    private int getIndexInSubPage(int index) {
        return index % subPageSizeInBytes;
    }

    private IMappedPage loadPage(int subPageIndex) {
        return pageCache.computeIfAbsent(subPageIndex, this::createPage);
    }

    private IMappedPage createPage(int subPageIndex) {
        checkClosed();
        checkPageCount(subPageIndex);
        try {
            if (subPageIndex >= pageCount) {
                pageCount = subPageIndex + 1;
            }
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, subPageIndex * subPageSizeInBytes, subPageSizeInBytes);
            return new MappedPage(mappedByteBuffer, subPageSizeInBytes);
        } catch (Exception e) {
            logger.error("create page failed", e);
        }
        return null;
    }
}
