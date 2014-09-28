package com.ctriposs.quickcache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.ctriposs.quickcache.util.TestUtil;

public class QuickCacheStressTest {

    private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/quickcache/";

    private static QuickCache<String> cache;

    public static void main(String[] args) throws IOException {
        int numKeyLimit = 1024 * 16;
        int valueLengthLimit = 1024 * 16;

        CacheConfig config = new CacheConfig();
        config.setStorageMode(CacheConfig.StorageMode.OffHeapFile)
                .setExpireInterval(2 * 1000)
                .setMigrateInterval(2 * 1000)
                .setMaxOffHeapMemorySize(5 * 10 * 1024 * 1024);
        cache = new QuickCache<String>(TEST_DIR, config);
        Map<String, byte[]> bytesMap = new HashMap<String, byte[]>();

        String[] rndStrings = new String[] {
                TestUtil.randomString(valueLengthLimit/2),
                TestUtil.randomString(valueLengthLimit),
                TestUtil.randomString(valueLengthLimit + valueLengthLimit/2)
        };
        byte[] bytes = rndStrings[1].getBytes();


    }
}
