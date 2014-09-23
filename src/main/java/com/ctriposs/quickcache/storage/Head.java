package com.ctriposs.quickcache.storage;

public class Head {
	
	private byte activeFlag;//0 means active;1 means used;

	public Head() {
		this.activeFlag = 1;
	}
	
	public Head(byte activeFlag) {
		this.activeFlag = activeFlag;
	}
	
	public byte getActiveFlag() {
		return activeFlag;
	}

	public void setActiveFlag(byte activeFlag) {
		this.activeFlag = activeFlag;
	}
	
}
