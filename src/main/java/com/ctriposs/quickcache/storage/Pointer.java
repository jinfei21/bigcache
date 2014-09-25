package com.ctriposs.quickcache.storage;

import com.ctriposs.quickcache.IBlock;

public class Pointer {

	private IBlock block;
	private int metaOffset;
	private long lastAccessTime = -1; // -1 means for delete.
	private long ttl = -1L;			  // -1 means for never expire.
	
	public Pointer(IBlock block,int metaOffset,long ttl) {
		this.block = block;
		this.metaOffset = metaOffset;
		this.ttl = ttl;
		this.lastAccessTime = System.currentTimeMillis();
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
}
