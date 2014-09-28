package com.ctriposs.quickcache;

import com.ctriposs.quickcache.util.TestSample;
import com.ctriposs.quickcache.util.TestUtil;
import com.ctriposs.quickcache.utils.FileUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    @Test
    public void testReadWriteOneMillion() throws Exception {
        final long count = 1000 * 1000;
        final int keyLen = 8;
        final int valueLen = 128;
        executeReadWrite(count, keyLen, valueLen);
    }

    @Test
    public void testReadWriteTwoMillion() throws Exception {
        final long count = 2 * 1000 * 1000;
        final int keyLen = 8;
        final int valueLen = 1024;
        executeReadWrite(count, keyLen, valueLen);
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

    @Test
    public void testMultiThreadWriteTtlTwoMillion() throws Exception {
        final long count = 2 * 1000 * 1000;
        final int threadCount = 16;
        ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        CacheConfig config = new CacheConfig();
        config.setCapacityPerBlock(20 * 1024 * 1024)
                .setExpireInterval(2 * 1000)
                .setMigrateInterval(2 * 1000);
        cache = new QuickCache<String>(TEST_DIR, config);

        long start = System.nanoTime();
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (int i = 0; i < threadCount; i++) {
            final int _i = i;
            futures.add(service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final TestSample sample = new TestSample();
                        StringBuilder sb = new StringBuilder();
                        for (int j = _i; j < count; j+= threadCount) {
                            sample.intA = j;
                            sample.doubleA = j;
                            sample.longA = j;
                            cache.put(TestSample.users(sb, j), sample.toBytes(), 2 * 1000);
                        }
                        Thread.sleep(10 * 1000);
                        for (int j = _i; j < count; j += threadCount) {
                            assertNull(cache.get(TestSample.users(sb, j)));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        long duration = System.nanoTime() - start;
        System.out.printf("Put/get %dK operations per second%n", (int) (count * 4 * 1e6 / duration));

        service.shutdown();
    }

    @After
    public void close() throws Exception {
        try {
            cache.close();
            FileUtil.deleteDirectory(new File(TEST_DIR));
        } catch (IOException e) {
            System.gc();
            try {
                FileUtil.deleteDirectory(new File(TEST_DIR));
            } catch (IOException e1) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e2){/**/}
                FileUtil.deleteDirectory(new File(TEST_DIR));
            }
        }
    }
}
