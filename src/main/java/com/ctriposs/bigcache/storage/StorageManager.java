package com.ctriposs.bigcache.storage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Managing a list of used/free storage blocks for cache operations like get/put/delete
 * 
 * @author bulldog
 *
 */
public class StorageManager implements IStorageBlock {
	
	/** keep track of the number of blocks allocated */
	private final AtomicInteger blockCount = new AtomicInteger(0);
	
	/**
	 * Directory for cache data store
	 */
	private final String dir;
	
	/**
	 * The capacity per block
	 * 
	 */
	private final int capacityPerBlock;
	
	
	/** The active storage block change lock. */
	private final Lock activeBlockChangeLock = new ReentrantLock();

	/**
	 *  A list of used storage blocks
	 */
	private final Queue<IStorageBlock> usedBlocks = new ConcurrentLinkedQueue<IStorageBlock>();
	
	/**
	 *  A queue of free storage blocks which is a priority queue and always return the block with smallest index.
	 */
	private final Queue<IStorageBlock> freeBlocks = new PriorityBlockingQueue<IStorageBlock>();
	
	/**
	 * Current active block for appending new cache data
	 */
	private volatile IStorageBlock activeBlock;
	
	/**
	 * The Constant DEFAULT_CAPACITY_PER_BLOCK.
	 */
	public final static int DEFAULT_CAPACITY_PER_BLOCK = 128 * 1024 * 1024; // 128M

	/** The Constant DEFAULT_INITIAL_NUMBER_OF_BLOCKS. */
	public final static int DEFAULT_INITIAL_NUMBER_OF_BLOCKS = 8; // 1GB total
	
	public StorageManager(String dir, int capacityPerBlock, int initialNumberOfBlocks) throws IOException {
		for(int i = 0; i < initialNumberOfBlocks; i++) {
			IStorageBlock storageBlock = new StorageBlock(dir, i, capacityPerBlock);
			freeBlocks.offer(storageBlock);
		}
		this.blockCount.set(initialNumberOfBlocks);
		this.activeBlock = freeBlocks.poll();
		this.usedBlocks.add(this.activeBlock);
        this.capacityPerBlock = capacityPerBlock;
        this.dir = dir;
	}

	@Override
	public byte[] retrieve(Pointer pointer) throws IOException {
		return pointer.getStorageBlock().retrieve(pointer);
	}

    @Override
	public byte[] remove(Pointer pointer) throws IOException {
		return pointer.getStorageBlock().remove(pointer);
	}

	@Override
	public Pointer store(byte[] payload) throws IOException {
		Pointer pointer = activeBlock.store(payload);
		if (pointer != null) return pointer; // success
		else { // overflow
			activeBlockChangeLock.lock(); 
			try {
				// other thread may have changed the active block
				pointer = activeBlock.store(payload);
				if (pointer != null) return pointer; // success
				else { // still overflow
					IStorageBlock freeBlock = this.freeBlocks.poll();
					if (freeBlock == null) { // create a new one
						freeBlock = new StorageBlock(this.dir, this.blockCount.getAndIncrement(), this.capacityPerBlock);
					}
					pointer = freeBlock.store(payload);
					this.activeBlock = freeBlock;
					this.usedBlocks.add(this.activeBlock);
					return pointer;
				}
				
			} finally {
				activeBlockChangeLock.unlock();
			}
		}
	}
	
	/**
	 * Stores the payload to the free storage block excluding the given block.
	 *
	 * @param payload the payload
	 * @param exludingBlock the storage block to be excluded
	 * @return the pointer
	 */
	public Pointer storeExcluding(byte[] payload, StorageBlock exludingBlock) throws IOException {
		while (this.activeBlock == exludingBlock) {
			activeBlockChangeLock.lock(); 
			try {
				// other thread may have changed the active block
				if (this.activeBlock != exludingBlock) break;
				IStorageBlock freeBlock = this.freeBlocks.poll();
				if (freeBlock == null) {
					freeBlock = new StorageBlock(this.dir, this.blockCount.getAndIncrement(), this.capacityPerBlock);
				}
				this.activeBlock = freeBlock;
				this.usedBlocks.add(this.activeBlock);
			} finally {
				activeBlockChangeLock.unlock();
			}
		}
		return store(payload);
	}

	@Override
	public Pointer update(Pointer pointer, byte[] payload) throws IOException {
		Pointer updatePointer = pointer.getStorageBlock().update(pointer, payload);
		if (updatePointer != null) {
			return updatePointer;
		}
		return pointer.copy(store(payload));
	}

	@Override
	public long getDirty() {
		long dirtyStorage = 0;
		for(IStorageBlock block : usedBlocks) {
			dirtyStorage += block.getDirty();
		}
		return dirtyStorage;
	}

	@Override
	public long getCapacity() {
		long totalCapacity = (usedBlocks.size() + freeBlocks.size()) * capacityPerBlock;
		return totalCapacity;
	}
	
	@Override
	public double getDirtyRatio() {
		return (this.getDirty() * 1.0) / this.getCapacity();
	}
	

	@Override
	public long getUsed() {
		long usedStorage = 0;
		for(IStorageBlock block : usedBlocks) {
			usedStorage += block.getUsed();
		}
		return usedStorage;
	}

	@Override
	public void free() {
        // safe?
		for(IStorageBlock storageBlock : usedBlocks) {
			storageBlock.free();
			this.freeBlocks.offer(storageBlock);
		}
		usedBlocks.clear();
		this.activeBlock = freeBlocks.poll();
		this.usedBlocks.add(this.activeBlock);
	}

    // only run by one thread.
    public void clean() {
        Iterator<IStorageBlock> it = usedBlocks.iterator();
        while(it.hasNext()) {
            IStorageBlock storageBlock = it.next();
            if (storageBlock == activeBlock) {
                // let active block be cleaned in the next run
                continue;
            }

            if (storageBlock.getUsed() == 0) {
                // we will not allocating memory from it any more and it is used by nobody.
                storageBlock.free();
                freeBlocks.add(storageBlock);
                it.remove();
            }
        }
    }

	@Override
	public void close() throws IOException {
		for(IStorageBlock usedBlock : usedBlocks) {
			usedBlock.close();
		}
        usedBlocks.clear();
		for(IStorageBlock freeBlock : freeBlocks) {
			freeBlock.close();
		}
        freeBlocks.clear();
	}

	@Override
	public int compareTo(IStorageBlock o) {
		throw new IllegalStateException("Not implemented!");
	}

	@Override
	public int getIndex() {
		throw new IllegalStateException("Not implemented!");
	}
	
	public int getFreeBlockCount() {
		return this.freeBlocks.size();
	}
	
	public int getUsedBlockCount() {
		return this.usedBlocks.size();
	}
	
	public int getTotalBlockCount() {
		return this.getFreeBlockCount() + this.getUsedBlockCount();
	}
}
