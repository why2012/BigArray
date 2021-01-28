package org.mine.iptable.repository;

public interface Repository extends AutoCloseable {
    int pageCount();
    byte[] fetchPage(int pageIndex);
    boolean savePage(int pageIndex, byte[] buf);
}
