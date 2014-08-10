package com.ctriposs.bigcache.storage;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

import com.ctriposs.bigcache.utils.FileUtil;
import com.ctriposs.bigcache.utils.TestUtil;

public class StorageManagerTest {
	
	private static String testDir = TestUtil.TEST_BASE_DIR + "unit/storage_manager_test/";
	
	private StorageManager storageManager = null;
	
	@Test
	public void testBasic() throws IOException {
		storageManager = new StorageManager(testDir, 1024 * 1024, 2); // 2M Total
		
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(0 == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		
		String testString = "Test String";
		byte[] testBytes = testString.getBytes();
		
		// store
		Pointer pointer = storageManager.store(testBytes);
		assertTrue(0L == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		assertTrue(0 == pointer.getPosition());
		assertTrue(testBytes.length == pointer.getLength());
		
		// retrieve
		byte[] resultBytes = storageManager.retrieve(pointer);
		assertEquals(testString, new String(resultBytes));
		assertTrue(0L == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		assertTrue(0 == pointer.getPosition());
		assertTrue(testBytes.length == pointer.getLength());
		
		// update to small
		String smallTestString = "Test Str";
		byte[] smallTestBytes = smallTestString.getBytes();
		pointer = storageManager.update(pointer, smallTestBytes);
		assertTrue((testBytes.length - smallTestBytes.length) == storageManager.getDirty());
		double expectedRatio = (testBytes.length - smallTestBytes.length) * 1.0 / storageManager.getCapacity();
		assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
		assertTrue(0 == pointer.getPosition());
		assertTrue(smallTestBytes.length == pointer.getLength());
		
		// update to bigger
		pointer = storageManager.update(pointer, testBytes);
		assertTrue(testBytes.length== storageManager.getDirty());
		expectedRatio = testBytes.length * 1.0 / storageManager.getCapacity();
		assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
		assertTrue(testBytes.length == pointer.getPosition());
		assertTrue(testBytes.length == pointer.getLength());
		
		// remove
		resultBytes = storageManager.remove(pointer);
		assertEquals(testString, new String(resultBytes));
		expectedRatio = testBytes.length * 2 * 1.0 / storageManager.getCapacity();
		assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
		
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		
		// free
		storageManager.free();
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(0L == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
	}
	
	@Test
	public void testlimitNunberOfItems() throws IOException {
		storageManager = new StorageManager(testDir, 1024 * 1024, 2); // 2M Total
		
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(0 == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		
		String testString = "Test String";
		byte[] testBytes = testString.getBytes();
		
		int limit = 1000;
		
		// store
		Pointer[] pointers = new Pointer[limit];
		for(int i = 0; i < limit; i++) {
			Pointer pointer = storageManager.store(testBytes);
			pointers[i] = pointer;
			assertTrue(0L == storageManager.getDirty());
			assertTrue(storageManager.getDirtyRatio() <= 1e-6);
			assertTrue(i * (testBytes.length) == pointer.getPosition());
			assertTrue(testBytes.length == pointer.getLength());
		}
		
		// retrieve
		for(int i = 0; i < limit; i++) {
			byte[] resultBytes = storageManager.retrieve(pointers[i]);
			assertEquals(testString, new String(resultBytes));
			assertTrue(0L == storageManager.getDirty());
			assertTrue(storageManager.getDirtyRatio() <= 1e-6);
			assertTrue(i * (testBytes.length) == pointers[i].getPosition());
			assertTrue(testBytes.length == pointers[i].getLength());
		}
		
		// update to small
		String smallTestString = "Test Str";
		byte[] smallTestBytes = smallTestString.getBytes();
		for(int i = 0; i < limit; i++) {
			pointers[i] = storageManager.update(pointers[i], smallTestBytes);
			assertTrue((i + 1) * (testBytes.length - smallTestBytes.length) == storageManager.getDirty());
			double expectedRatio = (i + 1) * (testBytes.length - smallTestBytes.length) * 1.0 / storageManager.getCapacity();
			assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
			assertTrue(i * (testBytes.length) == pointers[i].getPosition());
			assertTrue(smallTestBytes.length == pointers[i].getLength());
		}
		
		// update to bigger
		for(int i = 0; i < limit; i++) {
			pointers[i] = storageManager.update(pointers[i], testBytes);
			assertTrue(((testBytes.length - smallTestBytes.length) * (limit - i - 1))  + (i + 1) * testBytes.length == storageManager.getDirty());
			double expectedRatio = (((testBytes.length - smallTestBytes.length) * (limit - i - 1))  + (i + 1) * testBytes.length )* 1.0 / storageManager.getCapacity();
			assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
			assertTrue((limit + i) * testBytes.length == pointers[i].getPosition());
			assertTrue(testBytes.length == pointers[i].getLength());
		}
		
		// remove
		for(int i = 0; i < limit; i++) {
			byte[] resultBytes = storageManager.remove(pointers[i]);
			assertEquals(testString, new String(resultBytes));
			double expectedRatio = (testBytes.length * limit + testBytes.length * (i + 1)) * 1.0 / storageManager.getCapacity();
			assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
		}
		
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		
		
		// free
		storageManager.free();
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(0L == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testStoreOverflow() throws IOException {
		storageManager = new StorageManager(testDir, 1024 * 1024, 2); // 2M Total
		
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(0 == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		
		byte[] sourceBytes = new byte[1024];
		IStorageBlock previousBlock = null;
		for(int i = 0; i < 1024; i++) {
			Pointer pointer = storageManager.store(sourceBytes);
			assertNotNull(pointer);
			if (previousBlock == null) previousBlock = pointer.getStorageBlock();
			else {
				assertTrue(pointer.getStorageBlock() == previousBlock);
				previousBlock = pointer.getStorageBlock();
			}
		}
		
		Pointer pointer = storageManager.store(sourceBytes); // switch active block
		assertTrue(previousBlock != pointer.getStorageBlock());
		previousBlock = pointer.getStorageBlock();
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(0 == storageManager.getFreeBlockCount());
		assertTrue(2 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(0 == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		
		for(int i = 1; i < 1024; i++) {
			pointer = storageManager.store(sourceBytes);
			assertNotNull(pointer);
			if (previousBlock == null) previousBlock = pointer.getStorageBlock();
			else {
				assertTrue(pointer.getStorageBlock() == previousBlock);
				previousBlock = pointer.getStorageBlock();
			}
		}
		
		pointer = storageManager.store(sourceBytes); //switch active block
		assertTrue(previousBlock != pointer.getStorageBlock());
		previousBlock = pointer.getStorageBlock();
		assertTrue(3 == storageManager.getTotalBlockCount());
		assertTrue(0 == storageManager.getFreeBlockCount());
		assertTrue(3 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 3 == storageManager.getCapacity());
		assertTrue(0 == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testUpdateOverflow() throws IOException {
		storageManager = new StorageManager(testDir, 1024 * 1024, 2); // 2M Total
		
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(1 == storageManager.getFreeBlockCount());
		assertTrue(1 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(0 == storageManager.getDirty());
		assertTrue(storageManager.getDirtyRatio() <= 1e-6);
		
		byte[] sourceBytes = new byte[1024];
		IStorageBlock previousBlock = null;
		Pointer pointer = null;
		for(int i = 0; i < 1024; i++) {
			pointer = storageManager.store(sourceBytes);
			assertNotNull(pointer);
			if (previousBlock == null) previousBlock = pointer.getStorageBlock();
			else {
				assertTrue(pointer.getStorageBlock() == previousBlock);
				previousBlock = pointer.getStorageBlock();
			}
		}
		
		pointer = storageManager.update(pointer, new byte[512]);
		assertTrue(previousBlock == pointer.getStorageBlock()); // no switch
		
		pointer = storageManager.update(pointer, new byte[1024]);
		assertTrue(previousBlock != pointer.getStorageBlock()); // switch
		previousBlock = pointer.getStorageBlock();
		assertTrue(2 == storageManager.getTotalBlockCount());
		assertTrue(0 == storageManager.getFreeBlockCount());
		assertTrue(2 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
		assertTrue(1024 == storageManager.getDirty());
		double expectedRatio = 1024 * 1.0 / storageManager.getCapacity();
		assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
		
		for(int i = 1; i < 1024; i++) {
			pointer = storageManager.store(sourceBytes);
			assertNotNull(pointer);
			if (previousBlock == null) previousBlock = pointer.getStorageBlock();
			else {
				assertTrue(pointer.getStorageBlock() == previousBlock);
				previousBlock = pointer.getStorageBlock();
			}
		}
		
		pointer = storageManager.update(pointer, new byte[512]);
		assertTrue(previousBlock == pointer.getStorageBlock()); // no switch
		
		pointer = storageManager.update(pointer, new byte[1024]);
		assertTrue(previousBlock != pointer.getStorageBlock()); // switch
		previousBlock = pointer.getStorageBlock();
		
		assertTrue(3 == storageManager.getTotalBlockCount());
		assertTrue(0 == storageManager.getFreeBlockCount());
		assertTrue(3 == storageManager.getUsedBlockCount());
		assertTrue(1024 * 1024 * 3 == storageManager.getCapacity());
		assertTrue(1024 * 2 == storageManager.getDirty());
		expectedRatio = 1024 * 1.0 * 2 / storageManager.getCapacity();
		assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
	}
	
	@After
	public void clear() throws IOException {
		if (this.storageManager != null) {
			this.storageManager.close();
		}
		FileUtil.deleteDirectory(new File(testDir));
	}

}
