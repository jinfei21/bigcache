package com.ctriposs.quickcache.storage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.quickcache.CacheConfig.StorageMode;
import com.ctriposs.quickcache.IBlock;
import com.ctriposs.quickcache.IStorage;



public class StorageBlock implements IBlock {
	
	/** The index. */
	private final int index;
	
	/** The capacity. */
	private final int capacity;
	
	/** The underlying storage. */
	private IStorage underlyingStorage;
	
	/** The offset within the storage block. */
	private final AtomicInteger currentOffset = new AtomicInteger(0);
	
	/** The dirty storage. */
	private final AtomicInteger dirtyStorage = new AtomicInteger(0);
	
	/** The used storage. */
	private final AtomicInteger usedStorage = new AtomicInteger(0);
	
	
	/**
	 * Instantiates a new storage block.
	 *
	 * @param dir the directory
	 * @param index the index
	 * @param capacity the capacity
	 * @throws IOException exception throws when failing to create the storage block
	 */
	public StorageBlock(String dir, int index, int capacity, StorageMode storageMode) throws IOException{
		this.index = index;
		this.capacity = capacity;
		switch (storageMode) {
		case PureFile:
			underlyingStorage = new PureFileStorage(dir, index, capacity);
			break;
		case MapFile:
			underlyingStorage = new MapFileStorage(dir, index, capacity);
			break;
		case OffHeapFile:
			underlyingStorage = new OffHeapStorage(capacity);
			break;
		}
	}
	

	@Override
	public int compareTo(IBlock o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] retrieve(Pointer pointer) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] remove(Pointer pointer) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeLight(Pointer pointer) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Pointer store(byte[] key,byte[] payload,long ttl) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pointer update(Pointer pointer, byte[] payload) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int markDirty(Pointer pointer) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getDirty() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getUsed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCapacity() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDirtyRatio() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void free() {
		// TODO Auto-generated method stub
		
	}



}
