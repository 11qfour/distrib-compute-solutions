package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class EntityDao implements Dao<byte[]> {
    Map<String, byte[]> map = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isEmpty() || key.isBlank()) {
            throw new IllegalArgumentException();
        }
        var candidate = map.get(key);
        if (candidate == null) {
            throw new NoSuchElementException();
        }

        return map.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty() || key.isBlank()) {
            throw new IllegalArgumentException();
        }

        map.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty() || key.isBlank()) {
            throw new IllegalArgumentException();
        }

        map.remove(key);
    }

    @Override
    public void close() throws IOException {
        return;
    }
}
