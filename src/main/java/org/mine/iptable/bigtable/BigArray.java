package org.mine.iptable.bigtable;

public class BigArray implements AutoCloseable {
    private int pageSizeInBytes;
    private MappedPageFactory mappedPageFactory;

    private BigArray(int pageSizeInBytes, MappedPageFactory pageFactory) {
        this.pageSizeInBytes = pageSizeInBytes;
        mappedPageFactory = pageFactory;
    }

    public int pageSizeInBytes() {
        return pageSizeInBytes;
    }

    @Override
    public void close() throws Exception {
        mappedPageFactory.close();
    }

    public static class Builder {
        private final String dir;
        private int pageSizeInBytes;
        private int maxPageInMem;
        private int subPageSizeInBytes;
        private int maxSubPageInMem;

        public Builder(String dir) {
            this.dir = dir;
            this.pageSizeInBytes = 64 * 1024 * 1024; // 64MB
            this.maxPageInMem = -1;
            this.subPageSizeInBytes = 4 * 1024; // 4K
            this.maxSubPageInMem = 10;
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
            MappedPageFactory mappedPageFactory = new MappedPageFactory(dir, pageSizeInBytes, maxPageInMem, subPageSizeInBytes, maxSubPageInMem);
            return new BigArray(pageSizeInBytes, mappedPageFactory);
        }
    }

    public int getInt(long index) {
        index *= 4;
        return mappedPageFactory.getPage(getPageIndex(index)).getInt(getIndexInPage(index));
    }

    public void putInt(long index, int value) {
        index *= 4;
        mappedPageFactory.getPage(getPageIndex(index)).putInt(getIndexInPage(index), value);
    }

    public void putInt(int value) {
        mappedPageFactory.getPage().putInt(value);
    }

    private int getPageIndex(long index) {
        return (int)(index / pageSizeInBytes);
    }

    private int getIndexInPage(long index) {
        return (int)(index % pageSizeInBytes);
    }
}
