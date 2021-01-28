package org.mine.iptable;

import org.mine.iptable.bigtable.BigArray;
import org.mine.iptable.util.IpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Bootstrap {
    private final static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) throws Exception {
        String dir = ".\\data";
        /**
         * create a bigarray to store ip bits
         */
        BigArray bigArray = new BigArray.Builder(dir).pageSizeInBytes(64 * 1024 * 1024).maxPageCount(10).
                subPageSizeInBytes(1024 * 1024).maxSubPageInMem(10).build();
        /**
         * generate ip
         * 0.0.0.0 ~ 122.61.87.157
         */
        long ipCount = 149727279;
        String ipsPath = generateIps(dir, ipCount, false);
        /**
         * load ip into bigarray
         */
        loadIps(ipsPath, ipCount, bigArray);
        /**
         * generate ipList
         */
        List<String> ipList = getIpsToCheck();
        /**
         * determine whether these ip are in blacklist
         */
        determineBlackIp(ipList, bigArray);
        /**
         * print memory usage
         */
        stats();
        bigArray.close();
        bigArray.deletePages();
    }

    public static List<String> getIpsToCheck() {
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

    public static String generateIps(String dir, long count, boolean deleteIfExist) throws Exception {
        if (!dir.endsWith(File.separator)) {
            dir += File.separator;
        }
        String filepath = dir + "ips.txt";
        File ipsFile = new File(filepath);
        if (ipsFile.exists() && ipsFile.isFile()) {
            if (deleteIfExist) {
                ipsFile.delete();
            } else {
                logger.warn(filepath + " already exist, skip ips generating process");
                return filepath;
            }
        }
        logger.info("start generating ip");
        int threadCount = 20;
        long segCount = count / 20;
        long leftCount = count % 20;
        if (segCount == 0) {
            segCount = count;
            threadCount = 1;
            leftCount = 0;
        }
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            int startIndex = (int)(threadIndex * segCount);
            if (threadIndex == threadCount - 1) {
                segCount += leftCount;
            }
            final long segCountFinal = segCount;
            new Thread(() -> {
                try {
                    RandomAccessFile randomAccessFile = new RandomAccessFile(filepath, "rw");
                    randomAccessFile.seek(startIndex);
                    int ipBytes = startIndex;
                    for (long i = 0; i < segCountFinal; i++, ipBytes++) {
                        String ip = IpUtils.ip(ipBytes) + System.lineSeparator();
                        randomAccessFile.write(ip.getBytes());
                    }
                    randomAccessFile.close();
                } catch (Exception e) {
                    logger.error("generateIps", e);
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        logger.info("ip generating finished");
        return dir + "ips.txt";
    }

    public static void loadIps(String filepath, long ipCount, BigArray bigArray) throws Exception {
        logger.info("start loading ip");
        int threadCount = 20;
        long segCount = ipCount / 20;
        long leftCount = ipCount % 20;
        if (segCount == 0) {
            segCount = ipCount;
            threadCount = 1;
            leftCount = 0;
        }
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            int startIndex = (int) (threadIndex * segCount);
            if (threadIndex == threadCount - 1) {
                segCount += leftCount;
            }
            final long segCountFinal = segCount;
            new Thread(() -> {
                try {
                    RandomAccessFile randomAccessFile = new RandomAccessFile(filepath, "rw");
                    randomAccessFile.seek(startIndex);
                    String line;
                    long index = 0;
                    while (index++ < segCountFinal && (line = randomAccessFile.readLine()) != null) {
                        int ipBytes = IpUtils.parse(line);
                        int bytesIndex = IpUtils.byteIndicator(ipBytes);
                        int bitIndicator = IpUtils.bitIndicator(ipBytes);
                        byte mask = bigArray.getOrPutByte(bytesIndex);
                        mask |= bitIndicator;
                        bigArray.putByte(bytesIndex, mask);
                    }
                } catch (Exception e) {
                    logger.error("loadIps", e);
                } finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        logger.info("loading ip finished");
    }

    public static void stats() {
        long memUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.println("mem used " + (memUsed / 1024 / 1024) + " MB");
    }
}
