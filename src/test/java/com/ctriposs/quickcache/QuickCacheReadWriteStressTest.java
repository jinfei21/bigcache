package com.ctriposs.quickcache;

import com.ctriposs.quickcache.util.TestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class QuickCacheReadWriteStressTest {

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/quickcache/";

    private static QuickCache<String> cache;

    @Parameterized.Parameter(value = 0)
    public CacheConfig.StorageMode storageMode;

    @Parameterized.Parameters
    public static Collection<CacheConfig.StorageMode[]> data() {
        CacheConfig.StorageMode[][] data = {
                {CacheConfig.StorageMode.MapFile},
                {CacheConfig.StorageMode.OffHeapFile}
        };

        return Arrays.asList(data);
    }

    public QuickCache<String> cache(long count) throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode);
        QuickCache<String> cache = new QuickCache<String>(TEST_DIR, config);

        for (long i = 0; i < count; i++) {
            String key = "" + i;
            cache.put(key, key.getBytes());
        }

        return cache;
    }

    @Test
    public void testWriteTenMillion() throws Exception {
        final long count = 10 * 1000 * 1000;
        long elapsedTime = 0;
        long counter = 0;
        cache = cache(0);
        long startTime = System.nanoTime();

        for (long l = 0; l < count; l++) {
            String key = "" + l;
            long start = System.nanoTime();
            cache.put(key, key.getBytes());

            if (l % (100 * 1000) == 0) {
                elapsedTime += (System.nanoTime() - start);
                counter ++;
            }
        }

        System.out.println("avg:" + elapsedTime / counter + " ns");
        System.out.println("write " + count / (1000 * 1000) + " million times:" + (System.nanoTime() - startTime) / (1000 * 1000) + " ms");
    }

    @Test
    public void testReadTenMillion() throws Exception {
        final long count = 10 * 1000 * 1000;
        cache = cache(count);
        long startTime = System.nanoTime();

        for (long i = 0; i < count; i++) {
            String key = "" + i;
            cache.get(key);
        }
        System.out.println("read " + count / (1000 * 1000) + " million times:" + (System.nanoTime() - startTime) / (1000 * 1000) + " ms");
    }

    private void executeReadWrite(final long count, int keyLen, int valueLen) throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode);
        cache = new QuickCache<String>(TEST_DIR, config);
        List<String> keys = new ArrayList<String>();

        String key = TestUtil.randomString(keyLen);
        String value = TestUtil.randomString(valueLen);

        for (long l = 0; l < count; l++) {
            cache.put(key + l, TestUtil.getBytes(value + l));
            keys.add(key + l);
        }

        for (String k : keys) {
            String v = new String(cache.get(k));
            String index = k.substring(keyLen);
            assertEquals(v, value + index);
        }
        for (String k : keys) {
            cache.delete(k);
        }
        for (String k : keys) {
            assertNull(cache.get(k));
        }
    }
}
