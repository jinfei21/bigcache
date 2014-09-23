package com.ctriposs.quickcache.storage;

public class Meta {

	private static final int META_SIZE = (Integer.SIZE+Short.SIZE+Long.SIZE+Long.SIZE)/Byte.SIZE;
	public int keyOffSet;
	public short keySize;
	public long lastAccessTime;
	public long ttl;
	
	public Meta() {
		this.keyOffSet = 0;
		this.keySize = 0;
		this.lastAccessTime = 0;
		this.ttl = 0;
	}
	
	public static int getMetaSize() {
		return META_SIZE;
	}
	
	public static void main(String args[]) {
		System.out.println(getMetaSize());
	}
	
}
