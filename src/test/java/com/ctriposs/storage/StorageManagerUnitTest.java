package com.ctriposs.storage;

import com.ctriposs.quickcache.storage.Head;
import com.ctriposs.quickcache.storage.Meta;
import com.ctriposs.quickcache.storage.Pointer;
import com.ctriposs.quickcache.storage.StorageManager;
import com.ctriposs.quickcache.CacheConfig.*;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class StorageManagerUnitTest {
    private static String testDir = TestUtil.TEST_BASE_DIR + "unit/storage_manager_test/";

    private StorageManager storageManager = null;

    @Parameterized.Parameter(value = 0)
    public StorageMode storageMode;

    @Parameterized.Parameter(value = 1)
    public long size;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        Object[][] data = { { StorageMode.PureFile, 0 },
                { StorageMode.MapFile, 2 * 100 * 1024 * 1024 },
                { StorageMode.OffHeapFile, 2 * 100 * 1024 * 1024 } };
        return Arrays.asList(data);
    }

    @Test
    public void testBasic() throws IOException {
        storageManager = new StorageManager(testDir, 128 * 1024 * 1024, 2, storageMode, size, 0.5, StartMode.None);

        assertTrue(2 == storageManager.getTotalBlockCount());
        assertTrue(1 == storageManager.getFreeBlockCount());
        assertTrue(0 == storageManager.getUsedBlockCount());
        assertTrue(1024 * 1024 * 128 * 2 == storageManager.getCapacity());
        assertTrue(5L == storageManager.getDirty());
        assertTrue(Head.HEAD_SIZE == storageManager.getUsed());

        String testString = "Test String";
        String testKey = "Test Key";
        byte[] keyBytes = testKey.getBytes();
        byte[] testBytes = testString.getBytes();

        // store
        Pointer pointer = storageManager.store(keyBytes, testBytes, Meta.TTL_NEVER_EXPIRE);
        assertTrue(5L == storageManager.getDirty());
        assertTrue(storageManager.getUsedBlockCount() == 0);
        assertTrue(Head.HEAD_SIZE == pointer.getMetaOffset());
        assertTrue(keyBytes.length == pointer.getKeySize());
        assertTrue((testBytes.length + keyBytes.length + Head.HEAD_SIZE + Meta.META_SIZE) == storageManager.getUsed());

        // retrieve
        byte[] resultBytes = storageManager.retrieve(pointer);
        assertEquals(testString, new String(resultBytes));
        assertTrue(Head.HEAD_SIZE == pointer.getMetaOffset());
        assertTrue(keyBytes.length == pointer.getKeySize());
        assertTrue(testBytes.length == resultBytes.length);

        // update to small
        String smallTestString = "Test Str";
        byte[] smallTestBytes = smallTestString.getBytes();
        pointer = storageManager.store(keyBytes, smallTestBytes, Meta.TTL_NEVER_EXPIRE);
        assertTrue((testBytes.length + smallTestBytes.length + 2 * keyBytes.length + 2 * Meta.META_SIZE + Head.HEAD_SIZE) == storageManager.getUsed());
        assertTrue((Head.HEAD_SIZE + Meta.META_SIZE) == pointer.getMetaOffset());
        assertTrue(smallTestBytes.length == pointer.getValueSize());

        // update to bigger
        pointer = storageManager.store(keyBytes, testBytes, Meta.TTL_NEVER_EXPIRE);
        assertTrue(5L == storageManager.getDirty());
        assertTrue((Head.HEAD_SIZE + 2 * Meta.META_SIZE) == pointer.getMetaOffset());
        assertTrue(testBytes.length == pointer.getValueSize());
        assertTrue((2 * testBytes.length + smallTestBytes.length + 3 * keyBytes.length + 2 * Meta.META_SIZE + Head.HEAD_SIZE) == storageManager.getUsed());

        // remove
        resultBytes = storageManager.remove(pointer);
        assertEquals(testString, new String(resultBytes));
        assertTrue(resultBytes.length == storageManager.getDirty());

        assertTrue(2 == storageManager.getTotalBlockCount());
        assertTrue(1 == storageManager.getFreeBlockCount());
        assertTrue(0 == storageManager.getUsedBlockCount());
        assertTrue(1024 * 1024 * 128 * 2 == storageManager.getCapacity());

        // free
        storageManager.free();
        assertTrue(1024 * 1024 * 128 * 2 == storageManager.getCapacity());
        assertTrue(5L == storageManager.getDirty());
        assertTrue(2 == storageManager.getTotalBlockCount());
        assertTrue(2 == storageManager.getFreeBlockCount());
        assertTrue(0 == storageManager.getUsedBlockCount());
    }

   /* @Test
    public void testlimitNunberOfItems() throws IOException {
        storageManager = new StorageManager(testDir, 1024 * 1024, 2, storageMode, size); // 2M Total

        assertTrue(2 == storageManager.getTotalBlockCount());
        assertTrue(1 == storageManager.getFreeBlockCount());
        assertTrue(1 == storageManager.getUsedBlockCount());
        assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
        assertTrue(0 == storageManager.getDirty());
        assertTrue(storageManager.getDirtyRatio() <= 1e-6);
        assertTrue(0L == storageManager.getUsed());

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
        assertTrue(1000 * testBytes.length == storageManager.getUsed());

        // retrieve
        for(int i = 0; i < limit; i++) {
            byte[] resultBytes = storageManager.retrieve(pointers[i]);
            assertEquals(testString, new String(resultBytes));
            assertTrue(0L == storageManager.getDirty());
            assertTrue(storageManager.getDirtyRatio() <= 1e-6);
            assertTrue(i * (testBytes.length) == pointers[i].getPosition());
            assertTrue(testBytes.length == pointers[i].getLength());
        }
        assertTrue(1000 * testBytes.length == storageManager.getUsed());

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
        assertTrue(1000 * smallTestBytes.length == storageManager.getUsed());

        // update to bigger
        for(int i = 0; i < limit; i++) {
            pointers[i] = storageManager.update(pointers[i], testBytes);
            assertTrue(((testBytes.length - smallTestBytes.length) * (limit - i - 1))  + (i + 1) * testBytes.length == storageManager.getDirty());
            double expectedRatio = (((testBytes.length - smallTestBytes.length) * (limit - i - 1))  + (i + 1) * testBytes.length )* 1.0 / storageManager.getCapacity();
            assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
            assertTrue((limit + i) * testBytes.length == pointers[i].getPosition());
            assertTrue(testBytes.length == pointers[i].getLength());
        }
        assertTrue(1000 * testBytes.length == storageManager.getUsed());

        // remove
        for(int i = 0; i < limit; i++) {
            byte[] resultBytes = storageManager.remove(pointers[i]);
            assertEquals(testString, new String(resultBytes));
            double expectedRatio = (testBytes.length * limit + testBytes.length * (i + 1)) * 1.0 / storageManager.getCapacity();
            assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
        }
        assertTrue(0L == storageManager.getUsed());

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
        storageManager = new StorageManager(testDir, 1024 * 1024, 2, storageMode, size); // 2M Total

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
        assertTrue(1024 * 1024 == storageManager.getUsed());

        Pointer pointer = storageManager.store(sourceBytes); // switch active block
        assertTrue(previousBlock != pointer.getStorageBlock());
        previousBlock = pointer.getStorageBlock();
        assertTrue(2 == storageManager.getTotalBlockCount());
        assertTrue(0 == storageManager.getFreeBlockCount());
        assertTrue(2 == storageManager.getUsedBlockCount());
        assertTrue(1024 * 1024 * 2 == storageManager.getCapacity());
        assertTrue(0 == storageManager.getDirty());
        assertTrue(storageManager.getDirtyRatio() <= 1e-6);
        assertTrue(1025 * 1024 == storageManager.getUsed());

        for(int i = 1; i < 1024; i++) {
            pointer = storageManager.store(sourceBytes);
            assertNotNull(pointer);
            if (previousBlock == null) previousBlock = pointer.getStorageBlock();
            else {
                assertTrue(pointer.getStorageBlock() == previousBlock);
                previousBlock = pointer.getStorageBlock();
            }
        }
        assertTrue(2048 * 1024 == storageManager.getUsed());

        pointer = storageManager.store(sourceBytes); //switch active block
        assertTrue(previousBlock != pointer.getStorageBlock());
        previousBlock = pointer.getStorageBlock();
        assertTrue(3 == storageManager.getTotalBlockCount());
        assertTrue(0 == storageManager.getFreeBlockCount());
        assertTrue(3 == storageManager.getUsedBlockCount());
        assertTrue(1024 * 1024 * 3 == storageManager.getCapacity());
        assertTrue(0 == storageManager.getDirty());
        assertTrue(storageManager.getDirtyRatio() <= 1e-6);
        assertTrue(2049 * 1024 == storageManager.getUsed());
    }

    @SuppressWarnings("resource")
    @Test
    public void testUpdateOverflow() throws IOException {
        storageManager = new StorageManager(testDir, 1024 * 1024, 2, storageMode, size); // 2M Total

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
        assertTrue(1024 * 1024 == storageManager.getUsed());

        pointer = storageManager.update(pointer, new byte[512]);
        assertTrue(previousBlock == pointer.getStorageBlock()); // no switch
        assertTrue(1023 * 1024 + 512 == storageManager.getUsed());

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
        assertTrue(1024 * 1024 == storageManager.getUsed());

        for(int i = 1; i < 1024; i++) {
            pointer = storageManager.store(sourceBytes);
            assertNotNull(pointer);
            if (previousBlock == null) previousBlock = pointer.getStorageBlock();
            else {
                assertTrue(pointer.getStorageBlock() == previousBlock);
                previousBlock = pointer.getStorageBlock();
            }
        }
        assertTrue(2047 * 1024 == storageManager.getUsed());

        pointer = storageManager.update(pointer, new byte[512]);
        assertTrue(previousBlock == pointer.getStorageBlock()); // no switch
        assertTrue(2047 * 1024 - 512 == storageManager.getUsed());

        pointer = storageManager.update(pointer, new byte[1024]);
        assertTrue(previousBlock != pointer.getStorageBlock()); // switch
        previousBlock = pointer.getStorageBlock();
        assertTrue(2047 * 1024 == storageManager.getUsed());

        assertTrue(3 == storageManager.getTotalBlockCount());
        assertTrue(0 == storageManager.getFreeBlockCount());
        assertTrue(3 == storageManager.getUsedBlockCount());
        assertTrue(1024 * 1024 * 3 == storageManager.getCapacity());
        assertTrue(1024 * 2 == storageManager.getDirty());
        expectedRatio = 1024 * 1.0 * 2 / storageManager.getCapacity();
        assertTrue(Math.abs(expectedRatio - storageManager.getDirtyRatio()) <= 1e-6);
    }*/

    @After
    public void clear() throws IOException {
        if (this.storageManager != null) {
            this.storageManager.close();
        }

        try {
            FileUtil.deleteDirectory(new File(testDir));
        } catch (IllegalStateException e) {
            System.gc();
            try {
                FileUtil.deleteDirectory(new File(testDir));
            } catch (IllegalStateException e1) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e2) {
                }
                FileUtil.deleteDirectory(new File(testDir));
            }
        }
    }
}
