package org.mine.iptable.bigtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static org.mine.iptable.util.CommonUtils.resizeFor;

public class MappedPageFactory implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(MappedPageFactory.class);
    private final String indexDirectory;
    private final int pageSizeInBytes;
    private final int subPageSizeInBytes;
    private final int maxSubPageInMem;
    private final String PAGE_NAME_PREFIX;
    private final String PAGE_NAME_SUFFIX = ".dat";
    private final LRUCache<Integer, IMappedPage> pageCache;
    private volatile int pageCount = 0;

    public MappedPageFactory(String directory) {
        this(directory, "", 64 * 1024 * 1024, -1, 4 * 1024, 10);
    }

    public MappedPageFactory(String directory, int pageSizeInBytes) {
        this(directory, "", pageSizeInBytes, -1, 4 * 1024, 10);
    }

    public MappedPageFactory(String directory, String dataFilePrefix, int pageSizeInBytes, int maxPageInMem, int subPageSizeInBytes, int maxSubPageInMem) {
        if (subPageSizeInBytes > pageSizeInBytes) {
            throw new IllegalArgumentException("subPageSizeInBytes > pageSizeInBytes");
        }
        File dir = new File(directory);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("not a directory: " + directory);
        }
        if (!directory.endsWith(File.separator)) {
            directory += File.separator;
        }
        this.PAGE_NAME_PREFIX = dataFilePrefix + "page-";
        pageSizeInBytes = resizeFor(pageSizeInBytes);
        subPageSizeInBytes = resizeFor(subPageSizeInBytes);
        this.indexDirectory = directory;
        this.pageSizeInBytes = pageSizeInBytes;
        this.subPageSizeInBytes = subPageSizeInBytes;
        this.maxSubPageInMem = maxSubPageInMem;
        this.pageCache = new LRUCache<>(maxPageInMem);
        init();
    }

    public IMappedPage getPage(int index) {
        if (index >= pageCount || index < 0) {
            throw new IndexOutOfBoundsException("illegal index " + index + ", while pageSize=" + pageCount);
        }
        return loadPage(index);
    }

    public IMappedPage getOrCreatePage(int index) {
        if (index > pageCount || index < 0) {
            throw new IllegalArgumentException("illegal index " + index + ", while pageSize=" + pageCount);
        }
        return loadPage(index);
    }

    public IMappedPage getPage() {
        if (pageCount == 0) {
            return loadPage(0);
        } else {
            return loadPage(pageCount - 1);
        }
    }

    public int pageCount() {
        return pageCount;
    }

    private IMappedPage loadPage(int index) {
        return pageCache.computeIfAbsent(index, this::createPage);
    }

    private IMappedPage createPage(int index) {
        String indexPagePath = getIndexPagePath(index);
        File indexPageFile = new File(indexPagePath);
        if (indexPageFile.exists() && !indexPageFile.isFile()) {
            throw new IllegalStateException("index page already exist and is not a file: " + indexPagePath);
        }
        try  {
            RandomAccessFile randomAccessFile = new RandomAccessFile(indexPagePath, "rw");
            if (index >= pageCount) {
                pageCount = index + 1;
            }
            return new CompoundMappedPage(randomAccessFile, subPageSizeInBytes, pageSizeInBytes / subPageSizeInBytes, maxSubPageInMem);
        } catch (Exception e) {
            logger.error("create page failed", e);
        }
        return null;
    }

    private String getIndexPagePath(int index) {
        return indexDirectory + getIndexPageName(index);
    }

    private String getIndexPageName(int index) {
        String indexFileFormat = PAGE_NAME_PREFIX + "%d" + PAGE_NAME_SUFFIX;
        return String.format(indexFileFormat, index);
    }

    @Override
    public void close() throws Exception {
        pageCache.expireAll();
    }

    private void init() {
        File[] files = new File(indexDirectory).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile() && pathname.getName().startsWith(PAGE_NAME_PREFIX) && pathname.getName().endsWith(PAGE_NAME_SUFFIX);
            }
        });
        if (files == null || files.length == 0) {
            return;
        }
        for (File file: files) {
            String name = file.getName();
            int index = Integer.parseInt(name.substring(PAGE_NAME_PREFIX.length(), name.length() - PAGE_NAME_SUFFIX.length()));
            if (index < 0) {
                throw new RuntimeException("Illegal data file name: " + name);
            }
            loadPage(index);
        }
    }
}
