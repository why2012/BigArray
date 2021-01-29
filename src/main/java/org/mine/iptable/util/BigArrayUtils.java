package org.mine.iptable.util;

import org.mine.iptable.bigtable.BigArray;
import org.mine.iptable.bigtable.IMappedPage;
import org.mine.iptable.repository.Repository;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

public class BigArrayUtils {
    /**
     *
     * @param bigArray 待排序bigarray
     * @param availableCount 写入数
     * @param destDirectory 排序结果存储位置
     * @param resultPagePrefix 排序结果文件前缀，防止名称冲突
     * @return 排序结果
     */
    public static BigArray sortInt(BigArray bigArray, long availableCount, String destDirectory, String resultPagePrefix) {
        BigArray result = new BigArray.Builder(destDirectory, resultPagePrefix).pageSizeInBytes(bigArray.pageSizeInBytes()).
                maxPageCount(bigArray.maxPageCount()).subPageSizeInBytes(bigArray.subPageSizeInBytes()).maxSubPageInMem(64).build();
        sortPagesInt(bigArray, availableCount);
        int availablePageCount = (int)(availableCount * 4 / bigArray.pageSizeInBytes());
        int pageSizeIn4Bytes = bigArray.pageSizeInBytes() / 4;
        if (availableCount * 4 % bigArray.pageSizeInBytes() != 0) {
            availablePageCount++;
        }
        PriorityQueue<int[]> queue = new PriorityQueue<>(Comparator.comparingInt(v -> v[0]));
        int[] indexInPage = new int[availablePageCount];
        int[] pageSize = new int[availablePageCount];
        int[] offsetInPage = new int[availablePageCount];
        Arrays.fill(pageSize, bigArray.pageSizeInBytes() / 4);
        if (availableCount * 4 % bigArray.pageSizeInBytes() != 0) {
            pageSize[pageSize.length - 1] = (int) (availableCount * 4 % bigArray.pageSizeInBytes()) / 4;
        }
        for (int i = 1; i < availablePageCount; i++) {
            offsetInPage[i] = offsetInPage[i - 1] + pageSize[i - 1];
        }
        long processed = 0;
        long resultIndex = 0;
        if (processed < availableCount) {
            for (int i = 0; i < availablePageCount; i++) {
                queue.offer(new int[]{bigArray.getInt(offsetInPage[i] + indexInPage[i]++), i});
                processed++;
            }
        }
        while (!queue.isEmpty()) {
            int[] data = queue.peek();
            int pageIndex = data[1];
            if (indexInPage[pageIndex] < pageSize[pageIndex]) {
                int v = bigArray.getInt(offsetInPage[pageIndex] + indexInPage[pageIndex]++);
                if (v == data[0]) {
                    result.putInt(resultIndex++, v);
                } else {
                    queue.poll();
                    result.putInt(resultIndex++, data[0]);
                    queue.offer(new int[]{v, pageIndex});
                }
            } else {
                queue.poll();
                result.putInt(resultIndex++, data[0]);
            }
            processed++;
        }
        return result;
    }

    private static void sortPagesInt(BigArray bigArray, long availableCount) {
        int availablePageCount = (int)(availableCount * 4 / bigArray.pageSizeInBytes());
        int lastExtraBytes = (int)availableCount * 4 % bigArray.pageSizeInBytes();
        if (lastExtraBytes != 0) {
            availablePageCount++;
        }
        for (int i = 0; i < availablePageCount; i++) {
            int length = bigArray.pageSizeInBytes() / 4;
            if (i == availablePageCount - 1) {
                length = lastExtraBytes / 4;
            }
            IMappedPage mappedPage = bigArray.getMappedPage(i);
            mappedPage.force();
            int[] buf = mappedPage.load4Bytes(0, length);
            Arrays.sort(buf);
            bigArray.getMappedPage(i).put4Bytes(buf, 0, length);
        }
    }

    public static void saveToRepo(BigArray bigArray, Repository repository) {
        int pageCount = bigArray.pageCount();
        for (int i = 0; i < pageCount; i++) {
            IMappedPage mappedPage = bigArray.getMappedPage(i);
            byte[] buf = mappedPage.loadBytes(0, bigArray.pageSizeInBytes());
            repository.savePage(i, buf);
        }
    }

    public static void loadFromRepo(BigArray bigArray, Repository repository) {
        int pageCount = repository.pageCount();
        for (int i = 0; i < pageCount; i++) {
            IMappedPage mappedPage = bigArray.getMappedPageOrCreate(i);
            byte[] buf = repository.fetchPage(i);
            mappedPage.putBytes(buf, 0, buf.length);
        }
    }
}
