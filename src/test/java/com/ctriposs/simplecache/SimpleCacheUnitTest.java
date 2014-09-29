package com.ctriposs.simplecache;

import com.ctriposs.quickcache.CacheConfig;
import com.ctriposs.quickcache.SimpleCache;
import com.ctriposs.quickcache.util.TestUtil;
import com.ctriposs.quickcache.utils.FileUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class SimpleCacheUnitTest {

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "function/simplecache/";

    private static SimpleCache<Integer> cache;

    @Parameterized.Parameter(value = 0)
    public CacheConfig.StorageMode storageMode;

    @Parameterized.Parameters
    public static Collection<CacheConfig.StorageMode[]> data() {
        CacheConfig.StorageMode[][] data = {
                {CacheConfig.StorageMode.PureFile},
                {CacheConfig.StorageMode.MapFile},
                {CacheConfig.StorageMode.OffHeapFile}
        };

        return Arrays.asList(data);
    }

    public SimpleCache<Integer> cache() throws IOException {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode);
        cache = new SimpleCache<Integer>(TEST_DIR, config);
        cache.put(0, "A".getBytes());
        cache.put(1, "B".getBytes());
        cache.put(2, "C".getBytes());
        cache.put(3, "D".getBytes());
        cache.put(4, "E".getBytes());
        cache.put(5, "F".getBytes());
        return cache;
    }
/*
    @Test
    public void testGet() throws Exception {
        cache = cache();
        assertEquals(new String(cache.get(0)), "A");
        assertEquals(new String(cache.get(1)), "B");
        assertEquals(new String(cache.get(2)), "C");
        assertEquals(new String(cache.get(3)), "D");
        assertEquals(new String(cache.get(4)), "E");
        assertEquals(new String(cache.get(5)), "F");
    }
//*/    
/*
    @Test
    public void testPut() throws Exception {
        cache = cache();
        // test put
        cache.put(6, "G".getBytes());
        assertEquals(new String(cache.get(6)), "G");
        // test replace
        cache.put(0, "W".getBytes());
        assertEquals(new String(cache.get(0)), "W");
    }
//*/
/*    
    @Test
    public void testDelete() throws Exception {
        cache = cache();
        byte[] payload = cache.delete(0);
        assertEquals(new String(payload), "A");
        assertNull(cache.get(0));

        payload = cache.delete(6);
        assertNull(payload);
        assertNull(cache.get(6));
    }
//*/
/*
    @Test
    public void testContain() throws Exception {
        cache = cache();
        assertTrue(cache.contains(0));
        assertFalse(cache.contains(6));
    }
//*/
    @After
    public void close() throws IOException {
        if (cache == null)
            return;

        try {
            cache.close();
            FileUtil.deleteDirectory(new File(TEST_DIR));
        } catch (IOException e) {
            System.gc();
            try {
                FileUtil.deleteDirectory(new File(TEST_DIR));
            } catch (IOException e1) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e2) {/**/}

                FileUtil.deleteDirectory(new File(TEST_DIR));
            }
        }
    }
}
