package org.mine.iptable.bigtable;

public interface IMappedPage extends ICloseable {
    int getInt(int index);
    void putInt(int index, int v);
    void putInt(int v);
    byte getByte(int index);
    void putByte(int index, byte v);
    void putByte(byte v);
}
