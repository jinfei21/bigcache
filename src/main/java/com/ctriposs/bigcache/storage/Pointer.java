package com.ctriposs.bigcache.storage;

/**
 * The Class Pointer is a pointer to the stored cache data, which keeps
 * position and length of the payload and associated StorageBlock. Additionally,
 * it keeps track of time to idle and access time of the payload.
 */
public class Pointer {
	
	/** The position. */
	protected int position;
	
	/** The access time in milliseconds.
     *
     * The modifications of other fields are protected by
     */
	protected volatile long lastAccessTime; // -1 means for not initialized.
	
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
		this(position, length, -1, storageBlock);
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
		this.storageBlock = storageBlock;
        setLastAccessTime(accessTime);
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
     * we need to put the following restriction:
     * 1. We will always set the time to a bigger value than the current one.
     * 2. If the pointer has expired, don't set the value, so there won't be <em>expire</em> to <em>non-expire</em>, which
     * is wrong
	 *
	 * @param accessTime the new access time
     * @return true if we has modified the time successfully.
	 */
	public void setLastAccessTime(long accessTime) {
        synchronized(this) {
            if (lastAccessTime < 0) {
                // not initialized yet.
                lastAccessTime = accessTime;
                return;
            }

            // don't set it to an old value
            if (lastAccessTime >= accessTime) return;

            // can't update the access value if it has already expired.
            if (isExpired()) return;

            lastAccessTime = accessTime;
        }
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

    public Pointer copyWithoutAccessTime(Pointer pointer) {
        this.position = pointer.position;
        this.length = pointer.length;
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
        if (this.lastAccessTime < 0) return false; // not initialized
		return System.currentTimeMillis() - this.lastAccessTime > this.timeToIdle;
	}
}
