package org.mine.iptable.bigtable;

public interface IMappedPage extends ICloseable {
    int getInt(int index);
    void putInt(int index, int v);
    void putInt(int v);
    byte getByte(int index);
    void putByte(int index, byte v);
    void putByte(byte v);
    byte[] loadBytes(int offset, int length);
    void putBytes(byte[] buf, int offset, int length);
    int[] load4Bytes(int offset, int length);
    void put4Bytes(int[] buf, int offset, int length);
    void force();
}
