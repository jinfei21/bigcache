package com.ctriposs.quickcache.storage;


import com.ctriposs.quickcache.IBlock;

public class Pointer {

	private IBlock block;
	private int metaOffset;
	private int keySize;
	private int valueSize;
	private long lastAccessTime = -1; // -1 means for delete.
	private long ttl = -1L;			  // -1 means for never expire.
	
	public Pointer(IBlock block,int metaOffset,int keySize,int valueSize,long ttl) {
		this.block = block;
		this.metaOffset = metaOffset;
		this.ttl = ttl;
		this.lastAccessTime = System.currentTimeMillis();
		this.keySize = keySize;
		this.valueSize = valueSize;
	}

	public Pointer(IBlock block,int metaOffset,int keySize,int valueSize,long ttl, long lastAccessTime) {
		this(block, metaOffset, keySize, valueSize, ttl);
		this.lastAccessTime = lastAccessTime;
	}
	
	public IBlock getBlock() {
		return block;
	}

	public void setBlock(IBlock block) {
		this.block = block;
	}

	public int getMetaOffset() {
		return metaOffset;
	}

	public void setMetaOffset(int metaOffset) {
		this.metaOffset = metaOffset;
	}

	public long getLastAccessTime() {
		return lastAccessTime;
	}

	public void setLastAccessTime(long lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}
	
    /**
     * Is the cached item expired
     *
     * @return expired or not
     */
    public boolean isExpired() {
	    if (ttl <= 0) return false; 				// never expire
	    if (lastAccessTime < 0) return false; 		// not initialized
	    return System.currentTimeMillis() - lastAccessTime > ttl;
    }
    
	public int getKeySize() {
		return keySize;
	}


	public void setKeySize(int keySize) {
		this.keySize = keySize;
	}


	public int getValueSize() {
		return valueSize;
	}


	public void setValueSize(int valueSize) {
		this.valueSize = valueSize;
	}

	public int getItemSize() {
		return (this.keySize+this.valueSize);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Pointer) {
			Pointer other = (Pointer) obj;
			if (this.block == other.block && 
				this.lastAccessTime == other.lastAccessTime) {
				return true;
			}
		}
		return false;
	}
}
