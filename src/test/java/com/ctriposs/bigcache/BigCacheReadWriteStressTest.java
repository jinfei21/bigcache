package com.ctriposs.bigcache;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.ctriposs.bigcache.utils.FileUtil;
import com.ctriposs.bigcache.utils.TestUtil;

public class BigCacheReadWriteStressTest {

	private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "stress/bigcache/";

	private static BigCache<String> cache;

	public BigCache<String> cache(long count) throws IOException {
		CacheConfig config = new CacheConfig();
		BigCache<String> cache = new BigCache<String>(TEST_DIR, config);

		for (long i = 0; i < count; i++) {
			String key = "" + i;
			cache.put(key, key.getBytes());
		}
		return cache;
	}

	@Test
	public void testWrite_ten_million() throws IOException {
		final long item_count = 10 * 1000 * 1000;
		long elapsedTime = 0;
		long count = 0;
		cache = cache(0);
		long startTime = System.nanoTime();

		for (long i = 0; i < item_count; i++) {
			String key = "" + i;
			long start = System.nanoTime();
			cache.put(key, key.getBytes());

			if (i % (100 * 1000) == 0) {
				elapsedTime += (System.nanoTime() - start);
				count++;
			}
		}

		System.out.println("avg:" + elapsedTime / count + " ns");
		System.out.println("write " + item_count / (1000 * 1000) + " million times:"
				+ (System.nanoTime() - startTime) / (1000 * 1000) + " ms");
	}

	@Test
	public void testRead_ten_million() throws IOException {
		final long item_count = 10 * 1000 * 1000;
		cache = cache(item_count);
		long startTime = System.nanoTime();

		for (long i = 0; i < item_count; i++) {
			String key = "" + i;
			cache.get(key);
		}
		System.out.println("read " + item_count / (1000 * 1000) + " million times:" + (System.nanoTime() - startTime)
				/ (1000 * 1000)
				+ " ms");
	}

	@Test
	public void testReadWrite_one_million() throws IOException {
		final long item_count = 1000 * 1000;
		final int keyLen = 8;
		final int valueLen = 128;
		this.executeReadWrite(item_count, keyLen, valueLen);
	}

	@Test
	public void testReadWrite_two_million() throws IOException {
		final long item_count = 2 * 1000 * 1000;
		final int keyLen = 8;
		final int valueLen = 1024;
		this.executeReadWrite(item_count, keyLen, valueLen);
	}

	private void executeReadWrite(final long count, int keyLen, int valueLen) throws IOException {
		final int defaultKeyLen = 8;
		final int defaultValueLen = 32;

		CacheConfig config = new CacheConfig();
		cache = new BigCache<String>(TEST_DIR, config);
		List<String> keys = new ArrayList<String>();

		String key = "";
		String value = "";
		if (keyLen > 0) {
			key = TestUtil.randomString(keyLen);
		} else {
			key = TestUtil.randomString(defaultKeyLen);
		}
		if (valueLen > 0) {
			value = TestUtil.randomString(valueLen);
		} else {
			value = TestUtil.randomString(defaultValueLen);
		}
		for (int i = 0; i < count; i++) {
			cache.put(key + i, TestUtil.getBytes(value + i));
			keys.add(key + i);
		}
		assertEquals(cache.count(), count);
		for (String k : keys) {
			String v = new String(cache.get(k));
			String index = k.substring(keyLen > 0 ? keyLen : 8);
			assertEquals(value + index, v);
		}
		for (String k : keys) {
			cache.delete(k);
		}
		for (String k : keys) {
			assertNull(cache.get(k));
		}
	}

	@After
	public void close() throws IOException {
		try {
			cache.close();
			FileUtil.deleteDirectory(new File(TEST_DIR));
		} catch (IllegalStateException e) {
			System.gc();
			try {
				FileUtil.deleteDirectory(new File(TEST_DIR));
			} catch (IllegalStateException e1) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e2) {
				}
				FileUtil.deleteDirectory(new File(TEST_DIR));
			}
		}
	}

}
