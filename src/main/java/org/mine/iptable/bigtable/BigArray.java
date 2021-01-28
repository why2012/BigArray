package org.mine.iptable.bigtable;

public class BigArray implements AutoCloseable {
    private final int pageSizeInBytes;
    private final int maxPageCount;
    private MappedPageFactory mappedPageFactory;

    private BigArray(int pageSizeInBytes, int maxPageCount, MappedPageFactory pageFactory) {
        this.pageSizeInBytes = pageSizeInBytes;
        this.maxPageCount = maxPageCount;
        mappedPageFactory = pageFactory;
    }

    public int pageSizeInBytes() {
        return pageSizeInBytes;
    }

    public int maxPageCount() {
        return maxPageCount;
    }

    @Override
    public void close() throws Exception {
        mappedPageFactory.close();
    }

    public static class Builder {
        private final String dir;
        private final String dataFilePrefix;
        private int pageSizeInBytes;
        private int maxPageCount;
        private int maxPageInMem;
        private int subPageSizeInBytes;
        private int maxSubPageInMem;

        public Builder(String dir) {
            this(dir, "");
        }

        public Builder(String dir, String dataFilePrefix) {
            this.dir = dir;
            this.dataFilePrefix = dataFilePrefix;
            this.maxPageCount = 10;
            this.pageSizeInBytes = 64 * 1024 * 1024; // 64MB
            this.maxPageInMem = -1;
            this.subPageSizeInBytes = 1024 * 1024; // 1MB
            this.maxSubPageInMem = 10;
        }

        public Builder maxPageCount(int maxPageCount) {
            this.maxPageCount = maxPageCount;
            return this;
        }

        public Builder pageSizeInBytes(int pageSizeInBytes) {
            this.pageSizeInBytes = pageSizeInBytes;
            return this;
        }

        public Builder maxPageInMem(int maxPageInMem) {
            this.maxPageInMem = maxPageInMem;
            return this;
        }

        public Builder subPageSizeInBytes(int subPageSizeInBytes) {
            this.subPageSizeInBytes = subPageSizeInBytes;
            return this;
        }

        public Builder maxSubPageInMem(int maxSubPageInMem) {
            this.maxSubPageInMem = maxSubPageInMem;
            return this;
        }

        public BigArray build() {
            MappedPageFactory mappedPageFactory = new MappedPageFactory(dir, dataFilePrefix, pageSizeInBytes, maxPageInMem, subPageSizeInBytes, maxSubPageInMem);
            return new BigArray(pageSizeInBytes, maxPageCount, mappedPageFactory);
        }
    }

    public int getInt(long index) {
        index *= 4;
        checkIndex(index);
        return mappedPageFactory.getPage(getPageIndex(index)).getInt(getIndexInPage(index));
    }

    public int getOrPutInt(long index) {
        index *= 4;
        checkIndex(index);
        return mappedPageFactory.getOrCreatePage(getPageIndex(index)).getInt(getIndexInPage(index));
    }

    public void putInt(long index, int value) {
        index *= 4;
        checkIndex(index);
        mappedPageFactory.getOrCreatePage(getPageIndex(index)).putInt(getIndexInPage(index), value);
    }

    public void putInt(int value) {
        mappedPageFactory.getPage().putInt(value);
    }

    public byte getByte(long index) {
        checkIndex(index);
        return mappedPageFactory.getPage(getPageIndex(index)).getByte(getIndexInPage(index));
    }

    public byte getOrPutByte(long index) {
        checkIndex(index);
        return mappedPageFactory.getOrCreatePage(getPageIndex(index)).getByte(getIndexInPage(index));
    }

    public void putByte(long index, byte value) {
        checkIndex(index);
        mappedPageFactory.getOrCreatePage(getPageIndex(index)).putByte(getIndexInPage(index), value);
    }

    public void putByte(byte value) {
        mappedPageFactory.getPage().putByte(value);
    }

    private int getPageIndex(long index) {
        return (int)(index / pageSizeInBytes);
    }

    private int getIndexInPage(long index) {
        return (int)(index % pageSizeInBytes);
    }

    private void checkIndex(long index) {
        if (getPageIndex(index) + 1 > maxPageCount) {
            throw new IllegalArgumentException("index overflow, maxPageCount " + maxPageCount + ", required page index " + getPageIndex(index));
        }
    }
}
