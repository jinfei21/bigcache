package com.ctriposs.bigcache.storage;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import com.ctriposs.bigcache.utils.FileUtil;
import com.ctriposs.bigcache.utils.TestUtil;

public class MemoryMappedStorageTest {

	private static final String TEST_DIR = TestUtil.TEST_BASE_DIR + "unit/memory_mapped_storage_test/";
	private MemoryMappedStorage storage;

	public MemoryMappedStorage storage6() throws IOException {
		MemoryMappedStorage storage = new MemoryMappedStorage(TEST_DIR, 0, 16 * 1024 * 1024);
		storage.put(0, "A".getBytes());
		storage.put(1, "B".getBytes());
		storage.put(2, "C".getBytes());
		storage.put(3, "D".getBytes());
		storage.put(4, "E".getBytes());
		storage.put(5, "F".getBytes());
		return storage;
	}

	@Test
	public void testGet() throws IOException {
		storage = storage6();
		byte[] dest = new byte["A".getBytes().length];
		storage.get(0, dest);
		assertEquals(new String(dest), "A");
		storage.get(1, dest);
		assertEquals(new String(dest), "B");
		storage.get(2, dest);
		assertEquals(new String(dest), "C");
		storage.get(3, dest);
		assertEquals(new String(dest), "D");
		storage.get(4, dest);
		assertEquals(new String(dest), "E");
		storage.get(5, dest);
		assertEquals(new String(dest), "F");
	}

	@Test
	public void testPut() throws IOException {
		storage = storage6();
		//test put new
		storage.put(6, "G".getBytes());
		byte[] dest = new byte["A".getBytes().length];
		storage.get(6, dest);
		assertEquals(new String(dest), "G");
		//test replace old
		storage.put(0, "W".getBytes());
		storage.get(0, dest);
		assertEquals(new String(dest), "W");
	}

	@After
	public void clear() throws IOException {
		storage.close();
		try {
			FileUtil.deleteDirectory(new File(TEST_DIR));
		} catch (IllegalStateException e) {
			try {
				System.gc();
				FileUtil.deleteDirectory(new File(TEST_DIR));
			} catch (IllegalStateException e1) {
				try {
					Thread.sleep(3000);
					FileUtil.deleteDirectory(new File(TEST_DIR));
				} catch (InterruptedException e2) {
				}
			}
		}
	}
}
