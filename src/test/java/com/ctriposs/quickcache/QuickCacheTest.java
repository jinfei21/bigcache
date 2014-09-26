package com.ctriposs.quickcache;

import com.ctriposs.quickcache.util.TestUtil;
import com.ctriposs.quickcache.CacheConfig.*;
import com.ctriposs.quickcache.utils.FileUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class QuickCacheTest {
    
    private static final double STRESS_FACTOR = Double.parseDouble(System.getProperty("STRESS_FACTORY", "1.0"));
    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "function/quickcache/";
    
    private QuickCache<String> cache;
    
    @Parameterized.Parameter(value = 0)
    public StorageMode storageMode;

    @Parameterized.Parameters
    public static Collection<StorageMode[]> data() {
        StorageMode[][] data = {
                {StorageMode.PureFile},
                {StorageMode.MapFile},
                {StorageMode.OffHeapFile}
        };

        return Arrays.asList(data);
    }

    @Test
    public void testQuickCache() throws Exception {
        CacheConfig config = new CacheConfig();
        config.setStorageMode(storageMode);
        cache = new QuickCache<String>(TEST_DIR, config);
        Set<String> rndStringSet = new HashSet<String>();
        for (int i = 0; i < 2000000 * STRESS_FACTOR; i++) {
            String rndString = TestUtil.randomString(64);
            rndStringSet.add(rndString);
            cache.put(rndString, rndString.getBytes());
            if ((i % 50000) == 0 && i != 0) {
                System.out.println(i + " rows written");
            }
        }

        for (String rndString : rndStringSet) {
            byte[] value = cache.get(rndString);
            assertNotNull(value);
            assertEquals(rndString, new String(value));
        }

        // delete
        for (String rndString : rndStringSet) {
            cache.delete(rndString);
        }

        for (String rndString : rndStringSet) {
            byte[] value = cache.get(rndString);
            assertNull(value);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFileDir() {
        String fakeDir = "dltestDB://quickcache_test/asdl";
        CacheConfig config = new CacheConfig();
        try {
            cache = new QuickCache<String>(fakeDir, config);
        } catch (IOException e) {/**/}
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCacheConfig() {
        CacheConfig config = new CacheConfig().setCapacityPerBlock(12)
                        .setInitialNumberOfBlocks(27)
                        .setMaxOffHeapMemorySize(10);
    }

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
