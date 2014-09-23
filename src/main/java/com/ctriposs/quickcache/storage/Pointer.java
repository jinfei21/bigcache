package com.ctriposs.quickcache.storage;

import com.ctriposs.quickcache.IBlock;

public class Pointer {

	private IBlock block;
	private int metaIndex;
	private long lastAccessTime = -1; // -1 means for not initialized.
	private long ttl = -1L;			  // -1 means for never expire.
	
	public Pointer() {
		
	}


	public IBlock getBlock() {
		return block;
	}

	public void setBlock(IBlock block) {
		this.block = block;
	}

	public int getMetaIndex() {
		return metaIndex;
	}

	public void setMetaIndex(int metaIndex) {
		this.metaIndex = metaIndex;
	}

	public long getCreateTime() {
		return lastAccessTime;
	}

	public void setCreateTime(long createTime) {
		this.lastAccessTime = createTime;
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
