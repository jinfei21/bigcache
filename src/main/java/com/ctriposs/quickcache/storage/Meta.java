package com.ctriposs.quickcache.storage;

import java.io.Serializable;

public class Meta implements Serializable{
	public static final int DEFAULT_META_AREA_SIZE = 4 * 1024 * 1024; // 4M
	public static final int META_SIZE = (Integer.SIZE+Integer.SIZE+Integer.SIZE+Long.SIZE+Long.SIZE)/Byte.SIZE;
	public static final int KEY_OFFSET = 0;
	public static final int KEY_SIZE_OFFSET = 4;
	public static final int VALUE_SIZE_OFFSET = 8;
	public static final int LAST_ACCESS_OFFSET = 12;
	public static final int TTL_OFFSET = 20;
	public static final long TTL_NEVER_EXPIRE = -1L;
	public static final long TTL_DELETE = 0L;
	private int keyOffSet;
	private int keySize;
	private int valueSize;
	private long lastAccessTime;
	private long ttl;//-1 means never expire;0 means delete
	private int index = -1;//-1 means invalid
	public Meta() {
		this.keyOffSet = 0;
		this.keySize = 0;
		this.valueSize = 0;
		this.lastAccessTime = System.currentTimeMillis();
		this.ttl = 0;
	}
	
	public Meta(int index,int keyOffSet,int keySize,int valuesize,long lastAccessTime,long ttl) {
		this.keyOffSet = keyOffSet;
		this.keySize = keySize;
		this.lastAccessTime = lastAccessTime;
		this.ttl = ttl;
		this.index = index;
	}
	

	public int getIndex() {
		return index;
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

	public int getValueSize() {
		return valueSize;
	}

	public void setValueSize(int valueSize) {
		this.valueSize = valueSize;
	}

}
