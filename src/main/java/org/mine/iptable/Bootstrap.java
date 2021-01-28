package org.mine.iptable;

import org.mine.iptable.bigtable.BigArray;

public class Bootstrap {

    public static void main(String[] args) throws Exception {
        BigArray bigArray = new BigArray.Builder(".\\data").build();
        System.out.println(bigArray.getInt(0));
        System.out.println(bigArray.getInt(10));
        System.out.println(bigArray.getInt(34567));
        bigArray.putInt(0, 10);
        bigArray.putInt(10, 11);
        bigArray.putInt(34567, 12345);
        bigArray.putInt(bigArray.pageSizeInBytes() / 4 - 1, 25);
        System.out.println(bigArray.getInt(bigArray.pageSizeInBytes() / 4 - 1));
        System.out.println(bigArray.getInt(bigArray.pageSizeInBytes() * 2 / 4));
        bigArray.close();
    }
}
