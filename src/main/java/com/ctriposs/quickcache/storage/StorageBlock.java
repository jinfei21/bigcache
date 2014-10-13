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



public class StorageBlock implements IBlock {
	
	/** The index. */
	private final int index;
	
	/** The capacity. */
	private final int capacity;
	
	/** The meta capacity.*/
	private final int metaCapacity;
	
	/** The dirty storage. */
	private final AtomicInteger dirtyStorage = new AtomicInteger(0);
	
	/** The used storage. */
	private final AtomicInteger usedStorage = new AtomicInteger(0);
	
	/** The underlying storage. */
	private IStorage underlyingStorage;	
	
	/** The item offset within the active block.*/
	private final AtomicInteger currentItemOffset = new AtomicInteger(Meta.DEFAULT_META_AREA_SIZE);
	
	/** The meta offset within the active block.*/
	private final AtomicInteger currentMetaOffset = new AtomicInteger(0);
	
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
			this.underlyingStorage = new PureFileStorage(dir, index, capacity);
			break;
		case MapFile:
			this.underlyingStorage = new MapFileStorage(dir, index, capacity);
			break;
		case OffHeapFile:
			this.underlyingStorage = new OffHeapStorage(capacity);
			break;
		}
		this.metaCapacity = Meta.DEFAULT_META_AREA_SIZE;
	}
	
	/**
	 * Load existed storage block from file.
	 *
	 * @param file the directory
	 * @param index the index
	 * @param capacity the capacity
	 * @throws IOException exception throws when failing to create the storage block
	 */
	public StorageBlock(File file, int index, int capacity, StorageMode storageMode) throws IOException{
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
		this.metaCapacity = Meta.DEFAULT_META_AREA_SIZE;
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
		Pointer pointer = new Pointer(this, allocation.metaOffset, key.length, value.length, ttl);
		
		// write meta
		underlyingStorage.put(allocation.metaOffset + Meta.KEY_OFFSET, ByteUtil.toBytes(allocation.itemOffset));
		underlyingStorage.put(allocation.metaOffset + Meta.KEY_SIZE_OFFSET, ByteUtil.toBytes(key.length));
		underlyingStorage.put(allocation.metaOffset + Meta.VALUE_SIZE_OFFSET, ByteUtil.toBytes(value.length));
		underlyingStorage.put(allocation.metaOffset + Meta.LAST_ACCESS_OFFSET, ByteUtil.toBytes(pointer.getLastAccessTime()));
		underlyingStorage.put(allocation.metaOffset + Meta.TTL_OFFSET, ByteUtil.toBytes(ttl));
		// write item
		underlyingStorage.put(allocation.itemOffset, key);
		underlyingStorage.put(allocation.itemOffset + key.length, value);
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
		int metaOffset = currentMetaOffset.addAndGet(Meta.META_SIZE);
		if(capacity < itemOffset || metaCapacity < metaOffset){
			return null;
		}

        return new Allocation(itemOffset - payloadLength, metaOffset - Meta.META_SIZE);
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

		underlyingStorage.put(pointer.getMetaOffset() + Meta.TTL_OFFSET, ByteUtil.toBytes(Meta.TTL_DELETE));	
		byte bytes[] = new byte[4];
		underlyingStorage.get(pointer.getMetaOffset()+ Meta.KEY_OFFSET, bytes);
		int itemOffset = ByteUtil.ToInt(bytes);
		bytes = new byte[pointer.getValueSize()];		
		underlyingStorage.get(itemOffset + pointer.getKeySize(), bytes);
		dirtyStorage.addAndGet(pointer.getItemSize() + Meta.META_SIZE);
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
	
		/** The meta offset*/
		private int metaOffset;
		
		/**
		 * Instantiates a new allocation.
		 *
		 * @param itemOffset offset
		 * @param metaOffset offset
		 */
		public Allocation(int itemOffset, int metaOffset) {
			this.itemOffset = itemOffset;
			this.metaOffset = metaOffset;
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
        currentItemOffset.set(Meta.DEFAULT_META_AREA_SIZE);
        currentMetaOffset.set(0);
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

		int offset = index * Meta.META_SIZE;
		byte[] bytes = new byte[4];
		underlyingStorage.get(offset + Meta.KEY_OFFSET, bytes);
		int keyOffset = ByteUtil.ToInt(bytes);
		underlyingStorage.get(offset + Meta.KEY_SIZE_OFFSET, bytes);
		int keySize = ByteUtil.ToInt(bytes);
		underlyingStorage.get(offset + Meta.VALUE_SIZE_OFFSET, bytes);
		int valueSize = ByteUtil.ToInt(bytes);
		bytes = new byte[8];
		underlyingStorage.get(offset + Meta.LAST_ACCESS_OFFSET, bytes);
		long lastAccessTime = ByteUtil.ToLong(bytes);
		underlyingStorage.get(offset + Meta.TTL_OFFSET, bytes);
		long ttl = ByteUtil.ToLong(bytes);

		meta = new Meta(offset, keyOffset, keySize, valueSize, lastAccessTime,ttl);
		return meta;
	}
	
	@Override
	public List<Meta> getAllValidMeta() throws IOException {
		List<Meta> list = new ArrayList<Meta>();
		int useSize = 0;
		int i = 0;
		for (; i < Meta.MAX_META_COUNT; i++) {
			Meta meta = readMeta(i);
			if(0==meta.getLastAccessTime()) {
				break;
			}
			if ((System.currentTimeMillis() - meta.getLastAccessTime()) < meta.getTtl()||meta.getTtl() == Meta.TTL_NEVER_EXPIRE) {
				list.add(meta);
			}
			useSize += (meta.getKeySize()+meta.getValueSize()+Meta.META_SIZE);
			
		}
		dirtyStorage.set(capacity - useSize);
		usedStorage.set(useSize);
		if(getDirtyRatio() < QuickCache.DEFAULT_DIRTY_RATIO_THRESHOLD) {
			list.clear();
		}
		return list;
	}



	@Override
	public Item readItem(Meta meta) throws IOException {
		Item item = new Item();
		//read item
		byte[] bytes = new byte[meta.getKeySize()];
		underlyingStorage.get(meta.getKeyOffSet(), bytes);
		item.setKey(bytes);
		bytes = new byte[meta.getValueSize()];
		underlyingStorage.get(meta.getKeyOffSet()+meta.getKeySize(), bytes);
		item.setValue(bytes);
		return item;
	}

	@Override
	public int getMetaCount() throws IOException {
		int i=0;
		for (; i < Meta.MAX_META_COUNT; i++) {
			Meta meta = readMeta(i);
			if(0==meta.getLastAccessTime()) {
				break;
			}
		}
		return i;
	}

}
