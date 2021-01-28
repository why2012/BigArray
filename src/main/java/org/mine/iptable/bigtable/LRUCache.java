package org.mine.iptable.bigtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
        cache = new LinkedHashMap<K, T>(Math.max(3, Math.min(maxSize, 10)), 0.75f, true) {
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
                readLock.unlock();
                writeLock.lock();
                useWriteLock = true;
            }
            return cache.computeIfAbsent(key, creator);
        } finally {
            if (useWriteLock) {
                writeLock.unlock();
            } else {
                readLock.unlock();
            }
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

    public Set<T> values() {
        try {
            readLock.lock();
            Set<T> set = new HashSet<>();
            for (Map.Entry<K, T> entry: cache.entrySet()) {
                set.add(entry.getValue());
            }
            return set;
        } finally {
            readLock.unlock();
        }
    }

    public void expireAll() {
        try {
            writeLock.lock();
            Iterator<Map.Entry<K, T>> iter = cache.entrySet().iterator();
            while (iter.hasNext()){
                Map.Entry<K, T> entry = iter.next();
                if (!entry.getValue().isClosed()) {
                    entry.getValue().close();
                }
                iter.remove();
            }
        } catch (Exception e) {
            logger.error("expireAll error: ", e);
        } finally {
            writeLock.unlock();
        }
    }
}
