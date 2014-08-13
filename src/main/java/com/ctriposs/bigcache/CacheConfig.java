package com.ctriposs.bigcache;

import com.ctriposs.bigcache.storage.StorageManager;

public class CacheConfig {
	
	private int concurrencyLevel = BigCache.DEFAULT_CONCURRENCY_LEVEL;
	private int capacityPerBlock = StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
	private int initialNumberOfBlocks = StorageManager.DEFAULT_INITIAL_NUMBER_OF_BLOCKS;
    private long purgeInterval = BigCache.DEFAULT_PURGE_INTERVAL;
    private long maxMemoryStorageSize = StorageManager.DEFAULT_MEMORY_SIZE;
    private StorageMode storageMode = StorageMode.File;
	
	public int getConcurrencyLevel() {
		return concurrencyLevel;
	}

	public CacheConfig setConcurrencyLevel(int concurrencyLevel) {
		if(concurrencyLevel > 11 || concurrencyLevel < 0){
			throw new IllegalArgumentException("concurrencyLevel must be between 0 and 11 inclusive!");
		}
		
		this.concurrencyLevel = concurrencyLevel;
		return this;
	}

	public int getCapacityPerBlock() {
		return capacityPerBlock;
	}

	public CacheConfig setCapacityPerBlock(int capacityPerBlock) {
		if(capacityPerBlock < 16 * 1024 * 1024){
			throw new IllegalArgumentException("capacityPerBlock must be bigger than 16 * 1024 * 1024(16M)!");
		}
		
		this.capacityPerBlock = capacityPerBlock;
		return this;
	}

	public int getInitialNumberOfBlocks() {
		return initialNumberOfBlocks;
	}

	public CacheConfig setInitialNumberOfBlocks(int initialNumberOfBlocks) {
		
		if(initialNumberOfBlocks <= 0){
			throw new IllegalArgumentException("initialNumberOfBlocks must be > 0!");
		}
		
		this.initialNumberOfBlocks = initialNumberOfBlocks;
		return this;
	}

    public long getPurgeInterval() {
        return purgeInterval;
    }

    public CacheConfig setPurgeInterval(long purgeInterval) {
        this.purgeInterval = purgeInterval;
        return this;
    }

	public StorageMode getStorageMode() {
		return storageMode;
	}

	public CacheConfig setStorageMode(StorageMode storageMode) {
		this.storageMode = storageMode;
		return this;
	}

	public CacheConfig setStorageMode(StorageMode storageMode, long maxMemoryStorageSize) {
		if (maxMemoryStorageSize < 0 || maxMemoryStorageSize > 10 * 1000 * 1024 * 1024) {
			throw new IllegalArgumentException("maxMemoryStorageSize must be larger than 0 and smaller than 10GB!");
		}
		this.storageMode = storageMode;
		this.maxMemoryStorageSize = maxMemoryStorageSize;
		return this;
	}

	public long getMaxMemoryStorageSize() {
		return maxMemoryStorageSize;
	}

	public enum StorageMode {
		File,
		MemoryMappedWithFile,
		OffHeapWithFile,
	}
}
