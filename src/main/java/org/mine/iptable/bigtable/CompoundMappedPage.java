package org.mine.iptable.bigtable;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class CompoundMappedPage implements IMappedPage {
    private static Logger logger = LoggerFactory.getLogger(CompoundMappedPage.class);
    private final LRUCache<Integer, IMappedPage> pageCache;
    private final FileChannel fileChannel;
    private final int subPageSizeInBytes;
    private final int maxSubPage;
    private volatile boolean closed = false;
    private volatile int pageCount = 0;

    public CompoundMappedPage(FileChannel fileChannel, int subPageSizeInBytes, int maxSubPage, int maxSubPageInMem) {
        this.fileChannel = fileChannel;
        this.subPageSizeInBytes = subPageSizeInBytes;
        this.maxSubPage = maxSubPage;
        pageCache = new LRUCache<>(maxSubPageInMem);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        pageCache.expireAll();
        fileChannel.close();
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

    private void checkPageCount() {
        if (pageCount + 1 > maxSubPage) {
            throw new RuntimeException("maxSubPage " + maxSubPage + ", already used " + pageCount);
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
        loadPage(pageCount - 1).putInt(v);
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
        checkPageCount();
        try {
            if (subPageIndex >= pageCount) {
                pageCount = subPageIndex + 1;
            }
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, subPageIndex * subPageSizeInBytes, subPageSizeInBytes);
            return new MappedPage(mappedByteBuffer);
        } catch (Exception e) {
            logger.error("create page failed", e);
        }
        return null;
    }
}
