package org.mine.iptable.bigtable;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class LRUCache<K, T extends ICloseable> {
    private static final Logger logger = LoggerFactory.getLogger(LRUCache.class);
    private final int maxSize;
    private final Map<K, T> cache;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    public LRUCache(int maxSize) {
        this.maxSize = maxSize;
        cache = new LinkedHashMap<K, T>(Math.min(maxSize, 10), 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, T> eldest) {
                if (maxSize != -1 && size() > maxSize) {
                    try {
                        eldest.getValue().close();
                    } catch (Exception e) {
                        logger.error("remove entry error", e);
                    }
                    return true;
                }
                return false;
            }
        };
    }

    public T computeIfAbsent(K key, Function<K, T> creator) {
        boolean useWriteLock = false;
        try {
            readLock.lock();
            if (!cache.containsKey(key)) {
                writeLock.lock();
                useWriteLock = true;
            }
            return cache.computeIfAbsent(key, creator);
        } finally {
            if (useWriteLock) {
                writeLock.unlock();
            }
            readLock.unlock();
        }
    }

    public T put(K key, T value) {
        try {
            writeLock.lock();
            return cache.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    public T get(K key) {
        try {
            readLock.lock();
            return cache.get(key);
        } finally {
            readLock.unlock();
        }
    }

    public T remove(K key) {
        try {
            writeLock.lock();
            return cache.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    public void expireAll() {
        try {
            writeLock.lock();
            for (Map.Entry<K, T> entry: cache.entrySet()) {
                if (!entry.getValue().isClosed()) {
                    entry.getValue().close();
                }
            }
        } catch (Exception e) {
            logger.error("expireAll error: ", e);
        } finally {
            writeLock.unlock();
        }
    }
}
