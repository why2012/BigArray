package org.mine.iptable;

import org.mine.iptable.bigtable.BigArray;
import org.mine.iptable.util.IpUtils;

import java.util.*;
import java.io.File;
import java.io.RandomAccessFile;

public class Bootstrap {

    public static void main(String[] args) throws Exception {
        String dir = ".\\data";
        BigArray bigArray = new BigArray.Builder(dir).pageSizeInBytes(64 * 1024 * 1024).maxPageCount(10).
                subPageSizeInBytes(1024 * 1024).maxSubPageInMem(10).build();
        String ipsPath = generateIps(dir, 100);
        loadIps(ipsPath, bigArray);
        List<String> blackList = getIpInBlackList();
        determineBlackIp(blackList, bigArray);
        bigArray.close();
    }

    public static List<String> getIpInBlackList() {
        List<String> blackList = new LinkedList<>();
        blackList.add("0.0.0.1");
        blackList.add("0.0.0.99");
        blackList.add("0.0.12.12");
        blackList.add("192.168.0.1");
        return blackList;
    }

    public static void determineBlackIp(List<String> blackList, BigArray bigArray) {
        for (String ip: blackList) {
            int ipBytes = IpUtils.parse(ip);
            try {
                int bytesIndex = IpUtils.byteIndicator(ipBytes);
                int bitIndicator = IpUtils.bitIndicator(ipBytes);
                byte mask = bigArray.getByte(bytesIndex);
                boolean inblacklist = (mask & bitIndicator) != 0;
                if (inblacklist) {
                    System.out.println("ip " + ip + " is in blacklist");
                } else {
                    System.out.println("ip " + ip + " is not in blacklist");
                }
            } catch (IndexOutOfBoundsException e) {
                System.out.println("ip " + ip + " is not in blacklist");
            }
        }
    }

    public static String generateIps(String dir, int count) throws Exception {
        if (!dir.endsWith(File.separator)) {
            dir += File.separator;
        }
        String filepath = dir + "ips.txt";
        RandomAccessFile randomAccessFile = new RandomAccessFile(filepath, "rw");
        for (int i = 0; i < count; i++) {
            String ip = IpUtils.ip(i) + System.lineSeparator();
            randomAccessFile.write(ip.getBytes());
        }
        randomAccessFile.close();
        return dir + "ips.txt";
    }

    public static void loadIps(String filepath, BigArray bigArray) throws Exception {
        RandomAccessFile randomAccessFile = new RandomAccessFile(filepath, "rw");
        String line;
        while ((line = randomAccessFile.readLine()) != null) {
            int ipBytes = IpUtils.parse(line);
            int bytesIndex = IpUtils.byteIndicator(ipBytes);
            int bitIndicator = IpUtils.bitIndicator(ipBytes);
            byte mask = bigArray.getOrPutByte(bytesIndex);
            mask |= bitIndicator;
            bigArray.putByte(bytesIndex, mask);
        }
    }
}
