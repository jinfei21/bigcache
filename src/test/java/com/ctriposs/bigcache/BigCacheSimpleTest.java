package com.ctriposs.bigcache;

import com.ctriposs.bigcache.utils.TestUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by wenlu on 2014/8/12.
 */
public class BigCacheSimpleTest {
    private BigCache cache;

    @Before
    public void setup() {
        CacheConfig config = new CacheConfig();
        config.setCapacityPerBlock(16 * 1024 * 1024)
                .setConcurrencyLevel(10)
                .setInitialNumberOfBlocks(8)
                .setPurgeInterval(60 * 1000);
        try {
            cache = new BigCache(TestUtil.TEST_BASE_DIR, config);
        } catch (IOException e) {
            throw new RuntimeException("fail to create cache", e);
        }
    }

    @Test
    public void cacheDestroyTest() throws IOException {
        cache.put("aa", "bb".getBytes());
        cache.get("aa");
        TestUtil.sleepQuietly(1000);
        System.out.println(new String(cache.get("aa")));
    }

    @Test
    public void purgeTest() throws IOException {
        String testStr = "thisisfortest";
        cache.put("keywithoutttl", testStr.getBytes());
        cache.put("keywithttl", testStr.getBytes(), 2 * 1000);

        assertEquals(testStr, new String(cache.get("keywithttl")));
        assertEquals(testStr, new String(cache.get("keywithoutttl")));

        /**
         * sleep for 4 seconds, so the entry with ttl will expired
         */
        TestUtil.sleepQuietly(4 * 1000);
        assertEquals(testStr, new String(cache.get("keywithoutttl")));
        assertEquals(null, cache.get("keywithttl"));
        // still have 2 entries
        assertEquals(2, cache.pointerMap.size());

        // sleep a bit more than 1 minute for purge, and there is only one left
        TestUtil.sleepQuietly(65 * 1000);
        assertEquals(1, cache.pointerMap.size());

        // remove the only one entry
        assertTrue(cache.storageManager.getUsed() > 0);
        cache.delete("keywithoutttl");
        assertTrue(cache.storageManager.getUsed() == 0);
    }

    @Test
    public void moveTest() throws IOException {
        byte[] value = new byte[1000000]; // 1m-length value
        // 10 entry with ttl, 6 without ttl
        for (int i = 0; i < 10 ; i++) {
            cache.put("block1-keywithttl-" + i, value, 5 * 1000);
        }
        for (int i = 10; i < 16; i++) {
            cache.put("block1-keywithoutttl-" + i, value);
        }

        // do the same thing for the second block
        for (int i = 0; i < 10 ; i++) {
            cache.put("block2-keywithttl-" + i, value, 5 * 1000);
        }
        for (int i = 10; i < 16; i++) {
            cache.put("block2-keywithoutttl-" + i, value);
        }

        assertEquals(2, cache.storageManager.getUsedBlockCount());
        TestUtil.sleepQuietly(65000*2);
        assertEquals(1, cache.storageManager.getUsedBlockCount());
        assertEquals(12*1000*1000, cache.storageManager.getUsed());
    }

    @Test
    public void simplePerfTest() {
    }

    public static void main(String[] args) {
        try {
            BigCacheSimpleTest test = new BigCacheSimpleTest();
            test.setup();
            test.cacheDestroyTest();
            System.gc();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
