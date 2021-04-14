package org.geektimes.cache.redis;

import io.lettuce.core.codec.RedisCodec;

import javax.cache.CacheException;
import java.io.*;
import java.nio.ByteBuffer;

public class GenericRedisCodec<K, V> implements RedisCodec<K, V> {

    public GenericRedisCodec() {
    }

    @Override
    public K decodeKey(ByteBuffer bytes) {
        return (K) decode(bytes.array());
    }

    @Override
    public V decodeValue(ByteBuffer bytes) {
        return (V) decode(bytes.array());
    }

    @Override
    public ByteBuffer encodeKey(K key) {
        return ByteBuffer.wrap(encode(key));
    }

    @Override
    public ByteBuffer encodeValue(V value) {
        return ByteBuffer.wrap(encode(value));
    }

    private byte[] encode(Object value) throws CacheException {
        if (value == null) { return null; }
        byte[] bytes;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)
        ) {
            // Key -> byte[]
            objectOutputStream.writeObject(value);
            bytes = outputStream.toByteArray();
        } catch (IOException e) {
            throw new CacheException(e);
        }
        return bytes;
    }

    private Object decode(byte[] bytes) throws CacheException {
        if (bytes == null) { return null; }
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
             ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)
        ) {
            // byte[] -> Value
            return objectInputStream.readObject();
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }
}
