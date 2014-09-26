package com.ctriposs.quickcache.storage;

import java.util.concurrent.atomic.AtomicInteger;

public class Head {
	
	public static final int HEAD_SIZE = (Integer.SIZE + Byte.SIZE)/Byte.SIZE;
	public static final byte FLAG_ACTIVE = 1;
	public static final byte FLAG_DEACTIVE = 0;
	public static final int ACTIVE_OFFSET = 0;
	public static final int META_COUNT_OFFSET = 1;
	
	private byte activeFlag;    // 1 means active; 0 means used;
	private AtomicInteger metaCount;
	
	public Head() {
		this.activeFlag = FLAG_ACTIVE;
		this.metaCount = new AtomicInteger(0);
	}
	
	public Head(final byte activeFlag) {
		this.activeFlag = activeFlag;
		this.metaCount = new AtomicInteger(0);
	}
	
	public byte getActiveFlag() {
		return activeFlag;
	}

	public void setActiveFlag(final byte activeFlag) {
		this.activeFlag = activeFlag;
	}
	
	public int incMetaCount() {
		return this.metaCount.incrementAndGet();
	}
	
	public int getCurrentMetaCount() {
		return this.metaCount.get();
	}
	
	public void reset() {
		this.metaCount.set(0);
	}
	
}
