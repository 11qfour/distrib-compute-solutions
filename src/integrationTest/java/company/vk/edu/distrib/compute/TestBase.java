package company.vk.edu.distrib.compute;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Contains utility methods for unit tests.
 *
 */
abstract class TestBase {
    private static final int VALUE_LENGTH = 1024;

    static int randomPort() {
        return ThreadLocalRandom.current().nextInt(30000, 40000);
    }

    static String randomKey() {
        return Long.toHexString(ThreadLocalRandom.current().nextLong());
    }

    static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    static String endpoint(int port) {
        return "http://localhost:" + port;
    }
}
