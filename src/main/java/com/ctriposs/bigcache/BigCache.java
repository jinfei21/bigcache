package com.ctriposs.bigcache;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.bigcache.lock.StripedReadWriteLock;
import com.ctriposs.bigcache.storage.Pointer;
import com.ctriposs.bigcache.storage.StorageManager;
import com.ctriposs.bigcache.utils.FileUtil;

/**
 * The Class BigCache is a cache that uses persistent storage
 * to store or retrieve data in byte array. To do so,
 * BigCache uses pointers to point location(offset) of an item within a persistent storage block.
 * BigCache clears the storages periodically to gain free space if
 * storages are dirty(storage holes because of deletion). It also does eviction depending on
 * access time to the objects.
 * 
 * @param <K> the key type
 */
public class BigCache<K> implements ICache<K> {
	
	/** The Constant DELTA. */
	//private static final float DELTA = 0.00001f;
	
	/** The default storage block cleaning period which is 10 minutes. */
	public static final long DEFAULT_STORAGE_BLOCK_CLEANING_PERIOD = 10 * 60 * 1000;
	
	/** The default purge interval which is 10 minutes. */
	public static final long DEFAULT_PURGE_INTERVAL = 10 * 60 * 1000;

	/** The default storage block cleaning threshold. */
	public static final float DEFAULT_STORAGE_BLOCK_CLEANING_THRESHOLD = 0.5f;
	
	/** The Constant DEFAULT_CONCURRENCY_LEVEL. */
	public static final int DEFAULT_CONCURRENCY_LEVEL = 4;

	/** The hit. */
	protected AtomicLong hit = new AtomicLong();

	/** The miss. */
	protected AtomicLong miss = new AtomicLong();

	/** The pointer map. */
	protected ConcurrentMap<K, Pointer> pointerMap = new ConcurrentHashMap<K, Pointer>();

	/** Managing the storages. */
	private StorageManager storageManager;

	/** The read write lock. */
	private StripedReadWriteLock readWriteLock;

	/** The Constant NO_OF_CLEANINGS. */
	//private static final AtomicInteger NO_OF_CLEANINGS = new AtomicInteger();
	
	/** The Constant NO_OF_PURGES. */
	//private static final AtomicInteger NO_OF_PURGES = new AtomicInteger();
	
	/** The directory to store cached data */
	private String cacheDir;
	
	public BigCache(String dir, CacheConfig config) throws IOException {
		this.cacheDir = dir;
		if (!this.cacheDir.endsWith(File.separator)) {
			this.cacheDir += File.separator;
		}
		// validate directory
		if (!FileUtil.isFilenameValid(this.cacheDir)) {
			throw new IllegalArgumentException("Invalid cache data directory : " + this.cacheDir);
		}
		
		// clean up old cache data if exists
		FileUtil.deleteDirectory(new File(this.cacheDir));
		
		this.storageManager = new StorageManager(this.cacheDir, config.getCapacityPerBlock(), config.getInitialNumberOfBlocks());
		this.readWriteLock = new StripedReadWriteLock(config.getConcurrencyLevel());
	}
	

	@Override
	public void put(K key, byte[] value) throws IOException {
		this.put(key, value, -1); // -1 means no time to idle(never expires)
	}

	@Override
	public void put(K key, byte[] value, long tti) throws IOException {
		writeLock(key);
		try {
			Pointer pointer = pointerMap.get(key);
			if (pointer == null) {
				pointer = storageManager.store(value);
			} else {
				pointer = storageManager.update(pointer, value);
			}
			pointer.setTimeToIdle(tti);
			pointerMap.put(key, pointer);
		} finally {
			writeUnlock(key);
		}
	}

	@Override
	public byte[] get(K key) throws IOException {
		readLock(key);
		try {
			Pointer pointer = pointerMap.get(key);
			if (pointer != null) {
				hit.incrementAndGet();
				return storageManager.retrieve(pointer);
			} else {
				miss.incrementAndGet();
				return null;
			}
		} finally {
			readUnlock(key);
		}
	}

	@Override
	public byte[] delete(K key) throws IOException {
		writeLock(key);
		try {
			Pointer pointer = pointerMap.get(key);
			if (pointer != null) {
				byte[] payload = storageManager.remove(pointer);
				pointerMap.remove(key);
				return payload;
			}
		} finally {
			writeUnlock(key);
		}
		return null;
	}

	@Override
	public boolean contains(K key) {
		return pointerMap.containsKey(key);
	}

	@Override
	public void clear() {
		this.pointerMap.clear();
		this.storageManager.free();
	}

	@Override
	public double hitRatio() {
		return hit.get() / (hit.get() + miss.get());
	}
	
	/**
	 * Read Lock for key is locked.
	 * 
	 * @param key the key
	 */
	protected void readLock(K key) {
		readWriteLock.readLock(Math.abs(key.hashCode()));
	}

	/**
	 * Read Lock for key is unlocked.
	 * 
	 * @param key the key
	 */
	protected void readUnlock(K key) {
		readWriteLock.readUnlock(Math.abs(key.hashCode()));
	}

	/**
	 * Write Lock for key is locked..
	 * 
	 * @param key the key
	 */
	protected void writeLock(K key) {
		readWriteLock.writeLock(Math.abs(key.hashCode()));
	}

	/**
	 * Write Lock for key is unlocked.
	 * 
	 * @param key the key
	 */
	protected void writeUnlock(K key) {
		readWriteLock.writeUnlock(Math.abs(key.hashCode()));
	}

	@Override
	public void close() throws IOException {
		this.storageManager.close();
	}

}
