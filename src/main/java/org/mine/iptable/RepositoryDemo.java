package org.mine.iptable;

import org.mine.iptable.bigtable.BigArray;
import org.mine.iptable.repository.MysqlDBRepository;
import org.mine.iptable.repository.Repository;
import org.mine.iptable.util.BigArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class RepositoryDemo {
    private final static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) throws Exception {
        runBigArraySort(args);
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
        try (Repository repository = new MysqlDBRepository("jdbc:mysql://localhost:3306/bigarray?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&serverTimezone=Asia/Shanghai&connectTimeout=2000",
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
            System.out.println();
            bigArrayFromDB.close();
        } catch (Exception e) {
            logger.error("db op failed", e);
        }
        bigArray.close();
        bigArraySorted.close();
    }
}
