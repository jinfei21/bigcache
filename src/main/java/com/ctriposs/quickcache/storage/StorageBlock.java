package com.ctriposs.quickcache.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.quickcache.CacheConfig.StorageMode;
import com.ctriposs.quickcache.IBlock;
import com.ctriposs.quickcache.IStorage;
import com.ctriposs.quickcache.utils.ByteUtil;



public class StorageBlock implements IBlock {
	
	public static final int DEFAULT_META_AREA_SIZE = 4 * 1024 * 1024; // 4M

	/** The index. */
	private final int index;
	
	/** The capacity. */
	private final int capacity;
	
	/** The meta capacity.*/
	private final int metaCapacity;
	
	/** The dirty storage. */
	private final AtomicInteger dirtyStorage = new AtomicInteger(Head.HEAD_SIZE + DEFAULT_META_AREA_SIZE);
	
	/** The used storage. */
	private final AtomicInteger usedStorage = new AtomicInteger(0);
	
	/** The item offset within the storage block. */
	private final AtomicInteger currentItemOffset = new AtomicInteger(0);
	
	/** The meta offset within the storage block. */
	private final AtomicInteger currentMetaOffset = new AtomicInteger(0);
	
	/** The underlying storage. */
	private IStorage underlyingStorage;	
	
	/** The block head. */
	private final Head head = new Head();
	
	
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
		this.metaCapacity = DEFAULT_META_AREA_SIZE + Head.HEAD_SIZE;
		this.currentMetaOffset.set(Head.HEAD_SIZE);
		this.currentItemOffset.set(Head.HEAD_SIZE + DEFAULT_META_AREA_SIZE);
		deactive();
	}
	
	
	
	/**
	 * Stores the payload by the help of allocation.
	 *
	 * @param allocation the allocation
	 * @param key the key
	 * @return the pointer
	 * @throws IOException 
	 */
	public Pointer store(Allocation allocation, byte[] key,byte[] value,long ttl,int payloadLength) throws IOException {
		Pointer pointer = new Pointer(this,allocation.metaOffset,ttl);
		
		//write head
		underlyingStorage.put(Head.META_COUNT_OFFSET, ByteUtil.toBytes(head.incMetaCount()));
		
		//write meta	
		underlyingStorage.put(allocation.metaOffset + Meta.KEY_OFFSET, ByteUtil.toBytes(allocation.itemOffset));
		underlyingStorage.put(allocation.metaOffset + Meta.KEY_SIZE_OFFSET, ByteUtil.toBytes(key.length));
		underlyingStorage.put(allocation.metaOffset + Meta.LAST_ACCESS_OFFSET, ByteUtil.toBytes(pointer.getLastAccessTime()));
		underlyingStorage.put(allocation.metaOffset + Meta.TTL_OFFSET, ByteUtil.toBytes(ttl));
		//write item
		underlyingStorage.put(allocation.itemOffset + Item.VALUE_SIZE_OFFSET, ByteUtil.toBytes(value.length));
		underlyingStorage.put(allocation.itemOffset + Item.KEY_AREA_OFFSET, key);
		underlyingStorage.put(allocation.itemOffset + Item.KEY_AREA_OFFSET+key.length, value);
		//add stat
		usedStorage.addAndGet(payloadLength);
		return pointer;
	}

	@Override
	public Pointer store(byte[] key,byte[] value,long ttl) throws IOException {
		int payloadLength = key.length + value.length + 4;
		Allocation allocation = allocate(payloadLength);
		if (allocation == null) return null; // not enough storage available
		Pointer pointer = store(allocation, key,value,ttl,payloadLength);
		return pointer;
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
		if(capacity < itemOffset||metaCapacity < metaOffset){
			return null;
		}
		
		Allocation allocation = new Allocation(itemOffset - payloadLength, metaOffset - Meta.META_SIZE);
		return allocation;
	}


	@Override
	public byte[] retrieve(Pointer pointer) throws IOException {
		
		byte[] bytes = new byte[4];
		underlyingStorage.get(pointer.getMetaOffset() + Meta.KEY_OFFSET, bytes);
		int keyoffset = ByteUtil.ToInt(bytes);
		underlyingStorage.get(pointer.getMetaOffset() + Meta.KEY_SIZE_OFFSET, bytes);
		int keysize = ByteUtil.ToInt(bytes);
		underlyingStorage.get(keyoffset + Item.VALUE_SIZE_OFFSET, bytes);
		int valuesize = ByteUtil.ToInt(bytes);
		bytes = new byte[valuesize];
		underlyingStorage.get(keyoffset + Item.VALUE_SIZE_OFFSET+keysize, bytes);
		return bytes;
	}

	public byte[] remove(Pointer pointer) throws IOException {

		underlyingStorage.put(pointer.getMetaOffset() + Meta.LAST_ACCESS_OFFSET, ByteUtil.toBytes(Meta.LAST_ACCESS_DELETE));	
		byte[] bytes = new byte[4];
		underlyingStorage.get(pointer.getMetaOffset() + Meta.KEY_OFFSET, bytes);
		int keyoffset = ByteUtil.ToInt(bytes);
		underlyingStorage.get(pointer.getMetaOffset() + Meta.KEY_SIZE_OFFSET, bytes);
		int keysize = ByteUtil.ToInt(bytes);
		underlyingStorage.get(keyoffset + Item.VALUE_SIZE_OFFSET, bytes);
		int valuesize = ByteUtil.ToInt(bytes);
		bytes = new byte[valuesize];
		underlyingStorage.get(keyoffset + Item.VALUE_SIZE_OFFSET+keysize, bytes);
		
		int totalsize = valuesize+keysize+4;
		dirtyStorage.addAndGet(totalsize);
		
		return bytes;
	}


	@Override
	public int markDirty(Pointer pointer) throws IOException {
		byte[] bytes = new byte[4];
		underlyingStorage.get(pointer.getMetaOffset() + Meta.KEY_OFFSET, bytes);
		int keyoffset = ByteUtil.ToInt(bytes);
		underlyingStorage.get(pointer.getMetaOffset() + Meta.KEY_SIZE_OFFSET, bytes);
		int keysize = ByteUtil.ToInt(bytes);
		underlyingStorage.get(keyoffset + Item.VALUE_SIZE_OFFSET, bytes);
		int valuesize = ByteUtil.ToInt(bytes);
		int totalsize = valuesize+keysize+4;
		dirtyStorage.addAndGet(totalsize);
		return totalsize;
	}
	
	@Override
	public void close() throws IOException {
		if (this.underlyingStorage != null) {
			this.underlyingStorage.close();
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
		
		/**
		 * Gets the item offset.
		 *
		 * @return the offset
		 */
		public int getItemOffset() {
			return itemOffset;
		}
		
		/**
		 * Gets the meta offset.
		 *
		 * @return the offset
		 */
		public int getMetaOffset() {
			return metaOffset;
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
		currentMetaOffset.set(Head.HEAD_SIZE);
		currentItemOffset.set(Head.HEAD_SIZE+DEFAULT_META_AREA_SIZE);
		head.reset();
		dirtyStorage.set(Head.HEAD_SIZE+DEFAULT_META_AREA_SIZE);
		usedStorage.set(0);		
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



	@Override
	public void active() {
		byte[] source = new byte[1];
		try {
			source[0] = Head.FLAG_ACTIVE;
			this.underlyingStorage.put(Head.ACTIVE_OFFSET, source);
		} catch (IOException e) {
		}
	}



	@Override
	public void deactive() {
		byte[] source = new byte[1];
		try {
			source[0] = Head.FLAG_DEACTIVE;
			this.underlyingStorage.put(Head.ACTIVE_OFFSET, source);
		} catch (IOException e) {
		}
	}


	@Override
	public int getMetaCount() {
		return head.getCurrentMetaCount();
	}


	private Meta readMeta(int index) throws IOException {
		Meta meta = null;
		if(index < head.getCurrentMetaCount()) {
			
			int offset = Head.HEAD_SIZE + index*Meta.META_SIZE;
			byte[] bytes = new byte[4];
			underlyingStorage.get(offset + Meta.KEY_OFFSET, bytes);
			int keyoffset = ByteUtil.ToInt(bytes);
			underlyingStorage.get(offset + Meta.KEY_SIZE_OFFSET, bytes);
			int keysize = ByteUtil.ToInt(bytes);
			bytes = new byte[8];
			underlyingStorage.get(offset + Meta.LAST_ACCESS_OFFSET, bytes);
			long lastaccesstime = ByteUtil.ToLong(bytes);
			underlyingStorage.get(offset + Meta.TTL_OFFSET, bytes);
			long ttl = ByteUtil.ToLong(bytes);
			meta = new Meta(keyoffset,keysize,lastaccesstime,ttl);
		}
		return meta;
	}
	
	@Override
	public List<Meta> getAllValidMeta() throws IOException {
		List<Meta> list = new ArrayList<Meta>();
		for (int i = 0; i < head.getCurrentMetaCount(); i++) {
			Meta meta = readMeta(i);
			if (meta.getLastAccessTime() != Meta.LAST_ACCESS_DELETE
					&& System.currentTimeMillis() - meta.getLastAccessTime() < meta.getTtl()) {
				list.add(meta);
			}
		}

		return list;
	}



	@Override
	public Item readItem(Meta meta) throws IOException {
		Item item = new Item();
		
		//read item
		byte[] bytes = new byte[4];
		underlyingStorage.get(meta.getKeyOffSet() + Item.VALUE_SIZE_OFFSET, bytes);
		int valuesize = ByteUtil.ToInt(bytes);
		bytes = new byte[meta.getKeySize()];
		underlyingStorage.get(meta.getKeyOffSet() + Item.KEY_AREA_OFFSET, bytes);
		item.setKey(bytes);
		bytes = new byte[valuesize];
		underlyingStorage.get(meta.getKeyOffSet() + Item.KEY_AREA_OFFSET+meta.getKeySize(), bytes);
		item.setValue(bytes);
		return item;
	}

}
