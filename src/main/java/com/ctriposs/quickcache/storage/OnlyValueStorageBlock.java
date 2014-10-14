package com.ctriposs.quickcache.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.quickcache.CacheConfig.StorageMode;
import com.ctriposs.quickcache.IBlock;
import com.ctriposs.quickcache.IStorage;
import com.ctriposs.quickcache.QuickCache;
import com.ctriposs.quickcache.utils.ByteUtil;



public class OnlyValueStorageBlock implements IBlock {
	
	/** The index. */
	private final int index;
	
	/** The capacity. */
	private final int capacity;
	
	/** The dirty storage. */
	private final AtomicInteger dirtyStorage = new AtomicInteger(0);
	
	/** The used storage. */
	private final AtomicInteger usedStorage = new AtomicInteger(0);
	
	/** The underlying storage. */
	private IStorage underlyingStorage;	
	
	/** The item offset within the active block.*/
	private final AtomicInteger currentItemOffset = new AtomicInteger(0);

	/**
	 * Instantiates a new storage block.
	 *
	 * @param dir the directory
	 * @param index the index
	 * @param capacity the capacity
	 * @throws IOException exception throws when failing to create the storage block
	 */
	public OnlyValueStorageBlock(String dir, int index, int capacity, StorageMode storageMode) throws IOException{
		this.index = index;
		this.capacity = capacity;
		switch (storageMode) {
		case PureFile:
			this.underlyingStorage = new PureFileStorage(dir, index, capacity);
			break;
		case MapFile:
			this.underlyingStorage = new MapFileStorage(dir, index, capacity);
			break;
		case OffHeapFile:
			this.underlyingStorage = new OffHeapStorage(capacity);
			break;
		}
	
	}
	
	/**
	 * Load existed storage block from file.
	 *
	 * @param file the directory
	 * @param index the index
	 * @param capacity the capacity
	 * @throws IOException exception throws when failing to create the storage block
	 */
	public OnlyValueStorageBlock(File file, int index, int capacity, StorageMode storageMode) throws IOException{
		this.index = index;
		this.capacity = capacity;
		switch (storageMode) {
            case PureFile:
                this.underlyingStorage = new PureFileStorage(file, capacity);
                break;
            case MapFile:
                this.underlyingStorage = new MapFileStorage(file, capacity);
                break;
            case OffHeapFile:
                this.underlyingStorage = new OffHeapStorage(capacity);
                break;
		}

	}
	
	/**
	 * Stores the payload by the help of allocation.
	 *
	 * @param allocation the allocation
	 * @param payloadLength the payload
	 * @return the pointer
	 * @throws IOException 
	 */
	public Pointer store(Allocation allocation, byte[] key, byte[] value, long ttl, int payloadLength) throws IOException {
		Pointer pointer = new Pointer(this, 0, key.length, value.length, ttl);
	
		// write item
		underlyingStorage.put(allocation.itemOffset, value);
		// used storage update
		usedStorage.addAndGet(payloadLength + Meta.META_SIZE);

		return pointer;
	}

	@Override
	public Pointer store(byte[] key, byte[] value, long ttl) throws IOException {
		int payloadLength = key.length + value.length;
		Allocation allocation = allocate(payloadLength);
		if (allocation == null)
            return null; // not enough storage available

        return store(allocation, key, value, ttl, payloadLength);
	}

	/**
	 * Allocates storage for the payload, return null if not enough storage available.
	 *
	 * @param payloadLength the payload
	 * @return the allocation
	 */
	protected Allocation allocate(int payloadLength) {
		
		int itemOffset = currentItemOffset.addAndGet(payloadLength);
		if(capacity < itemOffset){
			return null;
		}

        return new Allocation(itemOffset - payloadLength);
	}


	@Override
	public byte[] retrieve(Pointer pointer) throws IOException {
		byte bytes[] = new byte[4];
		underlyingStorage.get(pointer.getMetaOffset() + Meta.KEY_OFFSET, bytes);
		int itemOffset = ByteUtil.ToInt(bytes);
		bytes = new byte[pointer.getValueSize()];
		underlyingStorage.get(itemOffset + pointer.getKeySize(), bytes);
		return bytes;
	}

    /**
     * Remove the pointer corresponding item (just mark dirty)
     * @param pointer the pointer
     * @return the byte array of value
     * @throws IOException
     */
	public byte[] remove(Pointer pointer) throws IOException {

		
		byte bytes[] = new byte[4];

		return bytes;
	}


	@Override
	public int markDirty(int dirtySize) {
		return dirtyStorage.addAndGet(dirtySize);
	}
	
	@Override
	public void close() throws IOException {
		if (underlyingStorage != null) {
			underlyingStorage.close();
		}
	}

	/**
	 * The Class Allocation.
	 */
	private static class Allocation {
		
		/** The item offset. */
		private int itemOffset;
		
		/**
		 * Instantiates a new allocation.
		 *
		 * @param itemOffset offset
		 */
		public Allocation(int itemOffset) {
			this.itemOffset = itemOffset;
		}
	}
	
	@Override
	public long getDirty() {
		return dirtyStorage.get();
	}

	@Override
	public long getUsed() {
		return usedStorage.get();
	}

	@Override
	public long getCapacity() {
		return capacity;
	}

	@Override
	public double getDirtyRatio() {
		return (getDirty() * 1.0) / getCapacity();
	}

	@Override
	public int getIndex() {
		return index;
	}

	@Override
	public void free() {
		dirtyStorage.set(0);
		usedStorage.set(0);
		currentItemOffset.set(0);
		underlyingStorage.free();
	}

	@Override
	public int compareTo(IBlock o) {
		if (getIndex() < o.getIndex()) {
			return -1;
		}else if (getIndex() == o.getIndex()) {
			return 0;
		}else {
			return 1;
		}
	}

	public Meta readMeta(int index) throws IOException {
		Meta meta = null;

		return meta;
	}
	
	@Override
	public List<Meta> getAllValidMeta() throws IOException {
		List<Meta> list = new ArrayList<Meta>();

		return list;
	}



	@Override
	public Item readItem(Meta meta) throws IOException {
		Item item = new Item();

		return item;
	}

	@Override
	public int getMetaCount() throws IOException {
		int i=0;

		return i;
	}

}
