package com.ctriposs.quickcache.storage;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ctriposs.quickcache.CacheConfig.StorageMode;
import com.ctriposs.quickcache.IBlock;
import com.ctriposs.quickcache.utils.FileUtil;

public class StorageManager implements IBlock{
	/**
	 * The Constant DEFAULT_CAPACITY_PER_BLOCK.
	 */
	public final static int DEFAULT_CAPACITY_PER_BLOCK = 128 * 1024 * 1024; // 128M

	/** The Constant DEFAULT_INITIAL_NUMBER_OF_BLOCKS. */
	public final static int DEFAULT_INITIAL_NUMBER_OF_BLOCKS = 8; // 1GB total

	/**
	 * The Constant DEFAULT_MEMORY_SIZE.
	 */
	public static final long DEFAULT_MAX_OFFHEAP_MEMORY_SIZE = 2 * 1024 * 1024 * 1024L; //Unit: GB
	
	
	/** keep track of the number of blocks allocated */
	private final AtomicInteger blockCount = new AtomicInteger(0);
	
	/** The active storage block change lock. */
	private final Lock activeBlockChangeLock = new ReentrantLock();

	/**
	 * Current active block for appending new cache data
	 */
	private volatile IBlock activeBlock;
	
	/**
	 *  A list of used storage blocks
	 */
	private final Queue<IBlock> usedBlocks = new ConcurrentLinkedQueue<IBlock>();
	
	/**
	 *  A queue of free storage blocks which is a priority queue and always return the block with smallest index.
	 */
	private final Queue<IBlock> freeBlocks = new PriorityBlockingQueue<IBlock>();
	
	/**
	 * Current storage mode
	 */
	private final StorageMode storageMode;
	
	/**
	 * The number of memory blocks allow to be created.
	 */
	private int allowedOffHeapModeBlockCount;
	
	/**
	 * Directory for cache data store
	 */
	private final String dir;
	
	/**
	 * The capacity per block in bytes
	 * 
	 */
	private final int capacityPerBlock;
	
	
	public StorageManager(String dir, int capacityPerBlock, int initialNumberOfBlocks, StorageMode storageMode,
			long maxOffHeapMemorySize) throws IOException {
		
		//TODO recovery from file
		// clean up old cache data if exists
		FileUtil.deleteDirectory(new File(dir));
		
		
		if (storageMode != StorageMode.PureFile) {
			this.allowedOffHeapModeBlockCount = (int)(maxOffHeapMemorySize / capacityPerBlock);
		} else {
			this.allowedOffHeapModeBlockCount = 0;
		}

		this.storageMode = storageMode;	
		this.capacityPerBlock = capacityPerBlock;
		this.dir = dir;
		
		for (int i = 0; i < initialNumberOfBlocks; i++) {
			IBlock storageBlock = createNewBlock(i);
			freeBlocks.offer(storageBlock);
		}
		
		this.blockCount.set(initialNumberOfBlocks);
		this.activeBlock = freeBlocks.poll();
	
	}
	
	private IBlock createNewBlock(int index) throws IOException {
		if (this.allowedOffHeapModeBlockCount > 0) {
			IBlock block = new StorageBlock(this.dir, index, this.capacityPerBlock, this.storageMode);
			this.allowedOffHeapModeBlockCount--;
			return block;
		} else {
			return new StorageBlock(this.dir, index, this.capacityPerBlock, StorageMode.PureFile);
		}
	}


	@Override
	public void close() throws IOException {
		for(IBlock usedBlock : usedBlocks) {
			usedBlock.close();
		}
        usedBlocks.clear();
		for(IBlock freeBlock : freeBlocks) {
			freeBlock.close();
		}
        freeBlocks.clear();
		
	}

	@Override
	public Pointer store(byte[] key,byte[] payload,long ttl) throws IOException {
		Pointer pointer = activeBlock.store(key,payload,ttl);
		if (pointer != null) {// success
			return pointer; 
		}else { // overflow
			activeBlockChangeLock.lock(); 
			try {
				// other thread may have changed the active block
				pointer = activeBlock.store(key,payload,ttl);
				if (pointer != null) return pointer; // success
				else { // still overflow
					IBlock freeBlock = this.freeBlocks.poll();
					if (freeBlock == null) { // create a new one
						freeBlock = this.createNewBlock(this.blockCount.getAndIncrement());
					}
					pointer = freeBlock.store(key,payload,ttl);
					this.activeBlock = freeBlock;
					this.usedBlocks.add(this.activeBlock);
					return pointer;
				}
				
			} finally {
				activeBlockChangeLock.unlock();
			}
		}
	}


	@Override
	public byte[] retrieve(Pointer pointer) throws IOException {
		
		return null;
	}


	@Override
	public byte[] remove(Pointer pointer) throws IOException {


		return null;
	}




	@Override
	public void removeLight(Pointer pointer) throws IOException {


		
	}



	@Override
	public Pointer update(Pointer pointer, byte[] payload) throws IOException {


		return null;
	}


	@Override
	public int markDirty(Pointer pointer) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getUsed() {
		long usedStorage = 0;
		for(IBlock block : usedBlocks) {
			usedStorage += block.getUsed();
		}
		return usedStorage;
	}

	@Override
	public void free() {
		for(IBlock storageBlock : usedBlocks) {
			storageBlock.free();
			this.freeBlocks.offer(storageBlock);
		}
		usedBlocks.clear();
		this.activeBlock.free();
	}
	
	@Override
	public long getDirty() {
		long dirtyStorage = 0;
		for(IBlock block : usedBlocks) {
			dirtyStorage += block.getDirty();
		}
		return dirtyStorage + activeBlock.getDirty();
	}

	@Override
	public long getCapacity() {
        long totalCapacity = 0;
        for(IBlock block : getAllInUsedBlocks()) {
            totalCapacity += block.getCapacity();
        }
		return totalCapacity;
	}

	@Override
	public double getDirtyRatio() {
		return (getDirty() * 1.0) / getCapacity();
	}


	@Override
	public int getIndex() {
		throw new IllegalStateException("Not implemented!");
	}

	@Override
	public int compareTo(IBlock o) {
		throw new IllegalStateException("Not implemented!");
	}

	
	public int getFreeBlockCount() {
		return freeBlocks.size();
	}
	
	public int getUsedBlockCount() {
		return usedBlocks.size()+1;
	}
	
    private Set<IBlock> getAllInUsedBlocks() {
        Set<IBlock> allBlocks = new HashSet<IBlock>();
        allBlocks.addAll(usedBlocks);
        allBlocks.addAll(freeBlocks);
        allBlocks.add(activeBlock);
        return allBlocks;
    }
	
	public int getTotalBlockCount() {
		return getAllInUsedBlocks().size();
	}
	
}
