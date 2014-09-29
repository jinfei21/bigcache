package com.ctriposs.quickcache.storage;

import java.util.concurrent.atomic.AtomicInteger;

public class Head {
	
	public static final int HEAD_SIZE = (Byte.SIZE + Integer.SIZE)/Byte.SIZE;
	public static final byte FLAG_ACTIVE = 1;
	public static final byte FLAG_USED = 2;
	public static final byte FLAG_FREE = 0;
	public static final int FLAG_OFFSET=0;
	public static final int META_COUNT_OFFSET=1;
	
	private byte activeFlag; //1 means active;2 means used;0 means free
	
	/** The current meta count*/
	private final AtomicInteger metaCount = new AtomicInteger(0);
	
	/** The item offset within the storage block. */
	private final AtomicInteger currentItemOffset = new AtomicInteger(Head.HEAD_SIZE+Meta.DEFAULT_META_AREA_SIZE);
	
	/** The meta offset within the meta data block. */
	private final AtomicInteger currentMetaOffset = new AtomicInteger(Head.HEAD_SIZE);
	
	public Head() {
		this.activeFlag = FLAG_FREE;
	}
	
	public Head(byte activeFlag,int metaCount) {
		this.activeFlag = activeFlag;
		this.metaCount.set(metaCount);
	}
	
	public byte getActiveFlag() {
		return activeFlag;
	}

	public int addAndGetCurrentItemOffset(int delta) {
		return this.currentItemOffset.addAndGet(delta);
	}
	
	public int addAndGetCurrentMetaOffset(int delta) {
		return this.currentMetaOffset.addAndGet(delta);
	}
	
	public void setCurrentItemOffset(int value) {
		this.currentItemOffset.set(value);
	}
	
	public void setCurrentMetaOffset(int value) {
		this.currentMetaOffset.set(value);
	}
	
	public void setActiveFlag(byte activeFlag) {
		this.activeFlag = activeFlag;
	}
	
	public int incMetaCount() {
		return this.metaCount.incrementAndGet();
	}
	
	public int getCurrentMetaCount() {
		return this.metaCount.get();
	}
	
	public void reset() {
		activeFlag = FLAG_FREE;
		currentMetaOffset.set(HEAD_SIZE);
		currentItemOffset.set(HEAD_SIZE + Meta.DEFAULT_META_AREA_SIZE);
		metaCount.set(0);
	}
	
}
