package company.vk.edu.distrib.compute.v11qfour;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class V11qfourMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        return storage.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(value);
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }

    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }

    private void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }
}
