package org.geektimes.cache.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import org.geektimes.cache.AbstractCache;
import org.geektimes.cache.ExpirableEntry;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import java.io.Serializable;
import java.util.Set;

public class LettuceCache<K extends Serializable, V extends Serializable> extends AbstractCache<K, V> {

    private final StatefulRedisConnection<K, V> connection;

    public LettuceCache(CacheManager cacheManager, String cacheName, Configuration<K, V> configuration,
                        StatefulRedisConnection<K, V> connection) {
        super(cacheManager, cacheName, configuration);
        this.connection = connection;
    }

    @Override
    protected boolean containsEntry(K key) throws CacheException, ClassCastException {
        return connection.sync().exists(key) == 1L;
    }

    @Override
    protected ExpirableEntry<K, V> getEntry(K key) throws CacheException, ClassCastException {
        return ExpirableEntry.of(key, connection.sync().get(key));
    }

    @Override
    protected void putEntry(ExpirableEntry<K, V> entry) throws CacheException, ClassCastException {
        connection.sync().set(entry.getKey(), entry.getValue());
    }

    @Override
    protected ExpirableEntry<K, V> removeEntry(K key) throws CacheException, ClassCastException {
        ExpirableEntry<K, V> old = getEntry(key);
        connection.sync().del(key);
        return old;
    }

    @Override
    protected void clearEntries() throws CacheException {
    }

    @Override
    protected Set<K> keySet() {
        return null;
    }

    @Override
    protected void doClose() {
        this.connection.close();
    }
}
