package com.ctriposs.quickcache.storage;

public class Item {
	
	private byte[] key;
	private int valueSize;
	private byte[] value;
	
	public Item(byte[] key,byte[] value) {
		this.key = key;
		this.value = value;
		this.valueSize = value.length;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public int getValueSize() {
		return valueSize;
	}

	public void setValueSize(int valueSize) {
		this.valueSize = valueSize;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

}
