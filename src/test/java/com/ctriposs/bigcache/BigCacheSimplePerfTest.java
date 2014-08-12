package com.ctriposs.bigcache;

import com.ctriposs.bigcache.utils.TestUtil;
import org.junit.Before;
import org.junit.Test;
import sun.java2d.SurfaceDataProxy;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Created by wenlu on 2014/8/12.
 */
public class BigCacheSimplePerfTest {
    private static DecimalFormat format = new DecimalFormat("###.### ###");
    private BigCache cache;

    @Before
    public void setUp() {
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

    public void simpleReadTest() {
    }

    @Test
    public void simpleWriteTest() {
        long count = 500;

        ForkJoinPool pool = new ForkJoinPool(16);
        WriteTask[] tasks = new WriteTask[16];
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < tasks.length; i++) {
            byte[] value = new byte[100000];
            for (int j = 0; j < value.length; j++) {
                value[j] = (byte)i;
            }
            tasks[i] = new WriteTask("write-"+i, cache, value, count, 60 * 1000, latch);
            pool.submit(tasks[i]);
        }

        latch.countDown();

        long totalTime = 0;
        for (int i = 0; i < tasks.length; i++) {
            try {
                totalTime += tasks[i].get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("------------------------");
        System.out.println(16 * count + " writes consumed " + totalTime + "nano seconds") ;
        System.out.println("one second for " + 1000000000.0/totalTime + " writes") ;
        System.out.println();
    }

    static abstract class CacheOpTask extends ForkJoinTask<Long> {
        protected final long ttl;
        protected final String name;
        protected final byte[] value;
        protected final long count;
        protected final BigCache<String> cache;
        private final CountDownLatch latch;

        private Long result;

        /**
         * The task with "name" will do operations on "count" copies of "value" into "cache", and the ttl of
         * each entry is "ttl"
         */
        CacheOpTask(String name, BigCache<String> cache, byte[] value, long count, long ttl, CountDownLatch latch) {
            this.name = name;
            this.value = value;
            this.cache = cache;
            this.count = count;
            this.ttl = ttl;
            this.latch = latch;
        }

        @Override
        public Long getRawResult() {
            return result;
        }

        @Override
        protected void setRawResult(Long value) {
            this.result = (Long) value;
        }

        @Override
        protected boolean exec() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long timeConsumed = 0;
            for (int i = 0; i < count; i++) {
                String key = name + "-" + i;
                try {
                    timeConsumed = doCacheOperation(key);
                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            setRawResult(timeConsumed);
            return true;
        }

        /**
         * Do operation with key and return the time consumed.
         */
        protected abstract long doCacheOperation(String key) throws IOException;
    }

    static class ReadTask extends CacheOpTask {

        ReadTask(String name, BigCache<String> cache, byte[] value, long count, long ttl, CountDownLatch latch) {
            super(name, cache, value, count, ttl, latch);
        }

        @Override
        protected long doCacheOperation(String key) throws IOException {
            long start = System.nanoTime();
            byte[] returnValue = cache.get(key);
            long end = System.nanoTime();

            if (!assertByteArrayEquals(value, returnValue)) {
                throw new IllegalStateException("invalid return value");
            }
            return end-start;
        }
    }

    static class WriteTask extends CacheOpTask {

        WriteTask(String name, BigCache<String> cache, byte[] value, long count, long ttl, CountDownLatch latch) {
            super(name, cache, value, count, ttl, latch);
        }

        @Override
        protected long doCacheOperation(String key) throws IOException {
            long start = System.nanoTime();
            cache.put(key, value, ttl);
            long end = System.nanoTime();

            return end-start;
        }
    }

    static boolean assertByteArrayEquals(byte[] left, byte[] right) {
        if (left != null && right != null) {
            if (left.length != right.length) return false;
            for(int i = 0; i < left.length; i++) {
                if (left[i] != right[i]) {
                    return false;
                }
            }
            return true;
        }

        if (left == null && right == null) return true;
        return false;
    }
}

