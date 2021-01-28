package org.mine.iptable.bigtable;

public interface ICloseable extends AutoCloseable {
    boolean isClosed();
}
