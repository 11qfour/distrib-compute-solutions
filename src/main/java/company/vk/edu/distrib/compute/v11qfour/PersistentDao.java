package company.vk.edu.distrib.compute.v11qfour;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;

public class PersistentDao implements Dao<byte[]> {
    private static final Path STORAGE_DIR = Path.of(".data-11qfour");
    private static final Logger log = LoggerFactory.getLogger(PersistentDao.class);

    public PersistentDao() throws IOException {
        if (!Files.exists(STORAGE_DIR)) {
            log.debug("Data storage does not exist yet, but will be created");
            Files.createDirectories(STORAGE_DIR);
        }
    }

    private Path getFilePath(String key) {
        validateKey(key);
        String saveFileName = Base64.getUrlEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
        String prefix = saveFileName.substring(0, Math.min(2, saveFileName.length()));
        Path dir = STORAGE_DIR.resolve(prefix);
        try {
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return dir.resolve(saveFileName);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path filePath = getFilePath(key);
        if (!Files.exists(filePath)) {
            return null;
        }
        return Files.readAllBytes(filePath);
    }

    @Override
    public synchronized void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateValue(value);
        Path finalPath = getFilePath(key);
        Path tempPath = finalPath.resolveSibling(finalPath.getFileName() + ".tmp");
        // Rewrite in temp file
        Files.write(tempPath, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        // Atomic rename, the file will not be half-written
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public synchronized void delete(String key) throws IllegalArgumentException, IOException {
        Path filePath = getFilePath(key);
        Files.deleteIfExists(filePath);
    }

    @Override
    public void close() {
        try (var stream = Files.walk(STORAGE_DIR)) {
            stream.filter(Files::isRegularFile)
                    .forEach(path -> {
                        String fileName = path.getFileName().toString();
                        if (fileName.endsWith(".tmp")) {
                            try {
                                Files.delete(path);
                                if (log.isDebugEnabled()) {
                                    log.debug("Deleted: {}", path);
                                }
                            } catch (IOException e) {
                                if (log.isErrorEnabled()) {
                                    log.error("Deletion error {}: {}", path, e.getMessage());
                                }
                            }
                        }
                    });
        } catch (IOException e) {
            log.error("Error walking storage directory", e);
        }
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
