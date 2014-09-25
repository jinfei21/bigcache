package com.ctriposs.quickcache.storage;

import java.io.Serializable;

public class Meta implements Serializable{

	public static final int META_SIZE = (Integer.SIZE + Integer.SIZE + Long.SIZE + Long.SIZE)/Byte.SIZE;
	public static final int KEY_OFFSET = 0;
	public static final int KEY_SIZE_OFFSET = 4;
	public static final int LAST_ACCESS_OFFSET = 8;
	public static final int TTL_OFFSET = 16;
	public static final long LAST_ACCESS_DELETE = -1L;
	
	private int keyOffSet;
	private int keySize;
	private long lastAccessTime;//-1 means delete
	private long ttl;
	
	public Meta() {
		this.keyOffSet = 0;
		this.keySize = 0;
		this.lastAccessTime = 0;
		this.ttl = 0;
	}
	
	public Meta(int keyOffSet,int keySize,long lastAccessTime,long ttl) {
		this.keyOffSet = keyOffSet;
		this.keySize = keySize;
		this.lastAccessTime = lastAccessTime;
		this.ttl = ttl;
	}
	

	public int getKeySize() {
		return keySize;
	}


	public void setKeySize(int keySize) {
		this.keySize = keySize;
	}


	public int getKeyOffSet() {
		return keyOffSet;
	}

	public void setKeyOffSet(int keyOffSet) {
		this.keyOffSet = keyOffSet;
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

}
