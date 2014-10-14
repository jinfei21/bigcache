package com.ctriposs.simplecache;

import com.ctriposs.quickcache.CacheConfig;
import com.ctriposs.quickcache.SimpleCache;
import com.ctriposs.quickcache.util.TestSample;
import com.ctriposs.quickcache.util.TestUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SimpleCachePerfTest {

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "performance/simplecache/";

    private static SimpleCache<String> cache;

    public static void main(String[] args) throws Exception {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(CacheConfig.StorageMode.PureFile)
                .setCapacityPerBlock(128 * 1024 * 1024)
                .setExpireInterval(2 * 1000)
                .setMigrateInterval(2 * 1000);
        cache = new SimpleCache<String>(TEST_DIR, config);

        final int count = 400 * 1000;
        final TestSample sample = new TestSample();
        long start = System.nanoTime();

        StringBuilder user = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sample.intA = i;
            sample.doubleA = i;
            sample.longA = i;
            cache.put(TestSample.users(user, i), sample.toBytes());
        }
        for (int i = 0; i < count; i++) {
            byte[] result = cache.get(TestSample.users(user, i));
            assertNotNull(result);
        }
        for (int i = 0; i < count; i++) {
            byte[] result = cache.get(TestSample.users(user, i));
            TestSample re = TestSample.fromBytes(result);
            assertEquals(i, re.intA);
            assertEquals(i, re.doubleA, 0.0);
            assertEquals(i, re.longA);
        }
        for (int i = 0; i < count; i++) {
            cache.delete(TestSample.users(user, i));
        }
        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %,d K operations per second%n",
                (int) (count * 4 * 1e6 / duration));
    }
}
