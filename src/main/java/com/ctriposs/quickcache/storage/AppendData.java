package com.ctriposs.quickcache.storage;

public class AppendData {
	
	public static final int LAST_ACCESS_OFFSET = 0;
	public static final int TTL_OFFSET = 8;
	public static final int KEY_SIZE_OFFSET = 16;
	public static final int VALUE_SIZE_OFFSET = 20;
	public static final int META_SIZE = (Integer.SIZE + Integer.SIZE + Long.SIZE + Long.SIZE) / Byte.SIZE;	


	private long lastAccessTime;
	private long ttl;   			//-1 means never expire;0 means delete	
	private byte[] key;
	private byte[] value;
	public AppendData(byte[] key,byte[] value,long lastAccessTime,long ttl) {
		this.key = key;
		this.value = value;
		this.lastAccessTime = lastAccessTime;
		this.ttl = ttl;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
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
