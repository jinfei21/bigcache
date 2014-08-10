package com.ctriposs.bigcache.storage;

/**
 * The Class Pointer is a pointer to the stored cache data, which keeps
 * position and length of the payload and associated StorageBlock. Additionally,
 * it keeps track of time to idle and access time of the payload.
 */
public class Pointer {
	
	/** The position. */
	protected int position;
	
	/** The access time in milliseconds. */
	protected long lastAccessTime;
	
	/** The length of the value. */
	protected int length;
	
	/** Time to idle in milliseconds */
	protected long timeToIdle = -1L;
	
	/** The associated storage block. */
	protected StorageBlock storageBlock;
	
	/**
	 * Instantiates a new pointer.
	 *
	 * @param position the position
	 * @param length the length of the value
	 * @param storageBlock the persistent cache storage
	 */
	public Pointer(int position, int length, StorageBlock storageBlock) {
		this(position, length, System.currentTimeMillis(), storageBlock);
	}
	
	/**
	 * Instantiates a new pointer.
	 *
	 * @param position the position
	 * @param length the length of the data
	 * @param accessTime the access time
	 * @param storageBlock the storage block
	 */
	public Pointer(int position, int length, long accessTime, StorageBlock storageBlock) {
		this.position = position;
		this.length = length;
		this.lastAccessTime = accessTime;
		this.storageBlock = storageBlock;
	}
	
	/**
	 * Gets the position.
	 *
	 * @return the position
	 */
	public int getPosition() {
		return position;
	}
	
	/**
	 * Sets the position.
	 *
	 * @param position the new position
	 */
	public void setPosition(int position) {
		this.position = position;
	}
	
	/**
	 * Gets the last access time.
	 *
	 * @return the access time
	 */
	public long getLastAccessTime() {
		return lastAccessTime;
	}
	
	/**
	 * Sets the last access time.
	 *
	 * @param accessTime the new access time
	 */
	public void setLastAccessTime(long accessTime) {
		this.lastAccessTime = accessTime;
	}
	
	/**
	 * Gets the storage block.
	 *
	 * @return the storage block.
	 */
	public StorageBlock getStorageBlock() {
		return storageBlock;
	}
	
	/**
	 * Sets the storage block.
	 *
	 * @param storageBlock the new storage block.
	 */
	public void setStorageBlock(StorageBlock storageBlock) {
		this.storageBlock = storageBlock;
	}

	/**
	 * Gets the length of the value
	 * 
	 * @return the length of the stored value
	 */
	public int getLength() {
		return length;
	}

	/**
	 * Sets the length of the stored value.
	 * 
	 * @param length the length of the stored value.
	 */
	public void setLength(int length) {
		this.length = length;
	}
	

	/**
	 * Gets the time to idle in milliseconds
	 * 
	 * @return time to idle
	 */
	public long getTimeToIdle() {
		return timeToIdle;
	}

	/**
	 * Sets the time to idle in milliseconds
	 * 
	 * @param timeToIdle the new time to idle
	 */
	public void setTimeToIdle(long timeToIdle) {
		this.timeToIdle = timeToIdle;
	}

	/**
	 * Copies given pointer.
	 *
	 * @param pointer the pointer
	 * @return the pointer
	 */
	public Pointer copy(Pointer pointer) {
		this.position = pointer.position;
		this.length = pointer.length;
		this.lastAccessTime = pointer.lastAccessTime;
		this.timeToIdle = pointer.timeToIdle;
		this.storageBlock = pointer.storageBlock;
		return this;
	}
	
	/**
	 * Is the cached item expired
	 * 
	 * @return expired or not
	 */
	public boolean isExpired() {
		if (this.timeToIdle <= 0) return false; // never expire
		return System.currentTimeMillis() - this.lastAccessTime > this.timeToIdle;
	}
}
