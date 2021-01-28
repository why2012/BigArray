package org.mine.iptable;

import org.mine.iptable.bigtable.BigArray;
import org.mine.iptable.repository.MysqlDBRepository;
import org.mine.iptable.repository.Repository;
import org.mine.iptable.util.BigArrayUtils;
import org.mine.iptable.util.IpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * 示例输出:
 * 32 28 8 17 6 16 47 17 2 14 20 34 8 20 3 0 5 6 21 48 27 37 30 6 32
 * 0 2 3 5 6 6 6 8 8 14 16 17 17 20 20 21 27 28 30 32 32 34 37 47 48
 * [main] INFO org.mine.iptable.Bootstrap - generating ip
 * [main] INFO org.mine.iptable.Bootstrap - generating ip finished, from 0.0.0.0 to 0.15.66.63
 * [main] INFO org.mine.iptable.Bootstrap - loading ip...(may be a bit slow)
 * [main] INFO org.mine.iptable.Bootstrap - loading ip finished
 * ip 0.0.0.1 is in blacklist
 * ip 0.0.0.99 is in blacklist
 * ip 0.0.12.12 is in blacklist
 * ip 0.1.134.159 is in blacklist
 * ip 192.168.0.1 is not in blacklist
 * ip 0.15.66.63 is in blacklist
 * mem used 13 MB
 * mem used 13 MB
 */
public class Bootstrap {
    private final static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) throws Exception {
        runBigArraySort(args);
        //runIpList(args);
    }

    public static void runBigArraySort(String[] args) throws Exception {
        String dir = ".\\datasort";
        BigArray bigArray = new BigArray.Builder(dir).pageSizeInBytes(10).maxPageCount(600).
                subPageSizeInBytes(5).maxSubPageInMem(8).build();
        int len = 25;
        Random random = new Random();
        for (int i = 0; i < len; i++) {
            bigArray.putInt(i, random.nextInt(50));
        }
        for (int i = 0; i < len; i++) {
            System.out.print(bigArray.getInt(i) + " ");
        }
        System.out.println();
        BigArray bigArraySorted = BigArrayUtils.sortInt(bigArray, len, dir, "sorted");
        for (int i = 0; i < len; i++) {
            System.out.print(bigArraySorted.getInt(i) + " ");
        }
        System.out.println();
        try (Repository repository = new MysqlDBRepository("jdbc:mysql://localhost:3306/bigarray_repo?useUnicode=true&characterEncoding=utf8",
                "root", "123456", "sorted_bigarray")) {
            // save to db
            logger.info("save bigarray to db");
            BigArrayUtils.saveToRepo(bigArraySorted, repository);
            // load from db
            logger.info("load bigarray from db");
            BigArray bigArrayFromDB = new BigArray.Builder(dir, "db").build();
            bigArrayFromDB.close();
            bigArrayFromDB.deletePages();
            bigArrayFromDB = new BigArray.Builder(dir, "db").pageSizeInBytes(10).maxPageCount(600).
                    subPageSizeInBytes(5).maxSubPageInMem(8).build();
            BigArrayUtils.loadFromRepo(bigArrayFromDB, repository);
            for (int i = 0; i < len; i++) {
                System.out.print(bigArrayFromDB.getInt(i) + " ");
            }
            bigArrayFromDB.close();
        } catch (Exception e) {
            logger.error("db op failed", e);
        }
        bigArray.close();
        bigArraySorted.close();
    }

    public static void runIpList(String[] args) throws Exception {
        String dir = ".\\data";
        /**
         * create a bigarray to store ip bits
         */
        BigArray bigArray = new BigArray.Builder(dir).pageSizeInBytes(1024 * 1024).maxPageCount(600).
                subPageSizeInBytes(128 * 1024).maxSubPageInMem(8).build();
        /**
         * generate ip
         * 0.0.0.0 ~ 0.15.66.63
         */
        String ipsPath = generateIps(dir, 1000000, false);
        /**
         * load ip into bigarray
         */
        loadIps(ipsPath, bigArray);
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
        stats();
    }

    public static List<String> getIpsToCheck() {
        List<String> blackList = new LinkedList<>();
        blackList.add("0.0.0.1");
        blackList.add("0.0.0.99");
        blackList.add("0.0.12.12");
        blackList.add("0.1.134.159");
        blackList.add("192.168.0.1");
        blackList.add("0.15.66.63");
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
        logger.info("generating ip");
        RandomAccessFile randomAccessFile = new RandomAccessFile(filepath, "rw");
        int ipBytes = 0;
        String ip = null;
        for (long i = 0; i < count; i++, ipBytes++) {
            ip = IpUtils.ip(ipBytes) + System.lineSeparator();
            randomAccessFile.write(ip.getBytes());
        }
        randomAccessFile.close();
        logger.info("generating ip finished, from 0.0.0.0 to " + ("" + ip).trim());
        return dir + "ips.txt";
    }

    public static void loadIps(String filepath, BigArray bigArray) throws Exception {
        if (bigArray.pageCount() > 0) {
            logger.info("page files already exsit, skip loading ip");
            return;
        }
        logger.info("loading ip...(may be a bit slow)");
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
        logger.info("loading ip finished");
    }

    public static void stats() {
        long memUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        System.out.println("mem used " + (memUsed / 1024 / 1024) + " MB");
    }
}
