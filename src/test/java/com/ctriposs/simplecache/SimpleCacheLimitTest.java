package com.ctriposs.simplecache;

import com.ctriposs.quickcache.CacheConfig;
import com.ctriposs.quickcache.QuickCache;
import com.ctriposs.quickcache.SimpleCache;
import com.ctriposs.quickcache.util.TestUtil;

import java.io.IOException;
import java.util.Date;

/**
 * Useless
 */
public class SimpleCacheLimitTest {

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/simplecache/";

    private static SimpleCache<String> cache;

    public static void main(String[] args) throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(CacheConfig.StorageMode.OffHeapFile)
                .setExpireInterval(2 * 1000)
                .setMigrateInterval(2 * 1000);
                //.setMaxOffHeapMemorySize(5 * 10 * 1024 * 1024); // 50M
        cache = new SimpleCache<String>(TEST_DIR, config);

        String rndString = TestUtil.randomString(10);

        System.out.println("Start from date " + new Date());
        long start = System.currentTimeMillis();
        for (long counter = 0; ; counter++) {
            cache.put(Long.toString(counter), rndString.getBytes());
            if (counter % 1000000 == 0) {
                System.out.println("Current date: " + new Date());
                System.out.println("counter: " + counter);
                System.out.println(TestUtil.getMemoryFootprint());
                long end = System.currentTimeMillis();
                System.out.println("timeSpent = " + (end - start));
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
                start = System.currentTimeMillis();
            }
        }
    }
}
