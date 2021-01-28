package org.mine.iptable.bigtable;

public interface IMappedPage extends ICloseable {
    int getInt(int index);
    void putInt(int index, int v);
    void putInt(int v);
}
