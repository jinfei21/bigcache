package com.ctriposs.bigcache;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.bigcache.lock.StripedReadWriteLock;
import com.ctriposs.bigcache.storage.Pointer;
import com.ctriposs.bigcache.storage.StorageBlock;
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
	protected final ConcurrentMap<K, Pointer> pointerMap = new ConcurrentHashMap<K, Pointer>();

	/** Managing the storages. */
	/* package for ut */ final StorageManager storageManager;

	/** The read write lock. */
	private final StripedReadWriteLock readWriteLock;

	/** The Constant NO_OF_CLEANINGS. */
	//private static final AtomicInteger NO_OF_CLEANINGS = new AtomicInteger();
	
	/** The Constant NO_OF_PURGES. */
	//private static final AtomicInteger NO_OF_PURGES = new AtomicInteger();
	
	/** The directory to store cached data */
	private String cacheDir;

    /** The thread pool which is used to clean the cache */
    private ScheduledExecutorService ses;

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
		
		this.storageManager = new StorageManager(this.cacheDir, config.getCapacityPerBlock(),
				config.getInitialNumberOfBlocks(), config.getStorageMode(), config.getMaxMemoryStorageSize());
		this.readWriteLock = new StripedReadWriteLock(config.getConcurrencyLevel());

        ses = new ScheduledThreadPoolExecutor(1);
        ses.scheduleWithFixedDelay(new CacheCleaner(this), config.getPurgeInterval(), config.getPurgeInterval(), TimeUnit.MILLISECONDS);
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
            if (pointer != null && pointer.isExpired()) {
                // expired, set the pointer to null as if we don't find it. This entry will be purged later
                pointer = null;
            }
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
	
	public long count(){
		return pointerMap.size();
	}

    static class CacheCleaner<K> implements Runnable {
        private WeakReference<BigCache> cacheHolder;
        private ScheduledExecutorService ses;

        CacheCleaner(BigCache<K> cache) {
            ses = cache.ses;
            this.cacheHolder = new WeakReference<BigCache>(cache);
        }

        @Override
        public void run() {
           BigCache cache = cacheHolder.get();
            if (cache == null) {
                // cache is recycled,
                if (ses != null) {
                    ses.shutdown();
                    ses = null;
                }
                return;
            }
            try {
                clean(cache);
            } catch (IOException e) {
                e.printStackTrace();
            }

            cache.storageManager.clean();
        }

        public void clean(BigCache cache) throws IOException {
            Set<K> keys = cache.pointerMap.keySet();

            for(K key : keys) {
                boolean expired = false;

                // 1. check with read lock
                cache.readLock(key);
                /**
                 * readLock is only used in "get" operation besides the clean thread. It's safe to modify
                 * the value of "pointer" here, as:
                 * a. the "get" thread will only change the access time and do no other modifications; and access
                 * time only affects expiration.
                 * b. there will be only one clean method, so it can do modifications to the storage of "pointer"(but it
                 * can't change the pointer reference)
                 */
                try {
                    Pointer pointer = (Pointer)cache.pointerMap.get(key);
                    if (pointer == null) {
                        // not exist now and do nothing, continue with next key;
                        continue;
                    }

                    expired = pointer.isExpired();
                    if (expired) {
                        // purge later with write lock
                    } else {
                        // check if the storage block has already been dirty
                        StorageBlock sb = pointer.getStorageBlock();
                        if (sb.getDirtyRatio() > 0.5) {
                            // the pointer is locked, and the current thread is the only one can affect the storage.
                            byte[] payload = cache.storageManager.retrieve(pointer, false);
                            cache.storageManager.remove(pointer);
                            Pointer newPointer = cache.storageManager.storeExcluding(payload, sb);

                            // the pointer object may be shared now, so we just modify its state.
                            // access time is volatile and we should use the old value.
                            long accesstime = pointer.getLastAccessTime();
                            pointer.copy(newPointer);
                            pointer.setLastAccessTime(accesstime);
                        }
                    }
                } finally {
                    cache.readUnlock(key);
                }

                // 2. expire with write lock internally if needed
                if (expired) {
                    cache.writeLock(key);
                    // the only thread with write key
                    try {
                        Pointer pointer = (Pointer)cache.pointerMap.get(key);
                        if (pointer == null) {
                            // already purged by others
                            ses.shutdown();
                            break;
                        }

                        if (pointer.isExpired()) {
                            cache.storageManager.remove(pointer);
                            cache.pointerMap.remove(key);
                        } else {
                            // may be refreshed by other threads
                        }
                    }finally {
                        cache.writeUnlock(key);
                    }
                }
            }
        }
    }
}
