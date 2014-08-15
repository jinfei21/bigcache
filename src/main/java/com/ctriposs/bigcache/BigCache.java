package com.ctriposs.bigcache;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
	
	/** The default purge interval which is 5 minutes. */
	public static final long DEFAULT_PURGE_INTERVAL = 5 * 60 * 1000;

    /** The default merge interval which is 10 minutes. */
    public static final long DEFAULT_MERGE_INTERVAL = 10 * 60 * 1000;

	/** The default storage block cleaning threshold. */
	public static final float DEFAULT_STORAGE_BLOCK_CLEANING_THRESHOLD = 0.5f;
	
	/** The Constant DEFAULT_CONCURRENCY_LEVEL. */
	public static final int DEFAULT_CONCURRENCY_LEVEL = 4;

    /** The default threshold for dirty block recycling */
    public static final double DEFAULT_DIRTY_RATIO_THRESHOLD = 0.5;

    /** The length of value can't be greater than 4m */
    public static final int MAX_VALUE_LENGTH = 4 * 1024 * 1024;

	/** The hit. */
	protected AtomicLong hit = new AtomicLong();

	/** The miss. */
	protected AtomicLong miss = new AtomicLong();

    /** The # of purges due to expiration. */
    protected AtomicLong purge = new AtomicLong();

    /** The # of moves for dirty block recycle. */
    protected AtomicLong move = new AtomicLong();

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

    /** dirty ratio which controls block recycle */
    private final double dirtyRatioThreshold;

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
				config.getInitialNumberOfBlocks(), config.getStorageMode(), config.getMaxOffHeapMemorySize());
		this.readWriteLock = new StripedReadWriteLock(config.getConcurrencyLevel());

        ses = new ScheduledThreadPoolExecutor(2);
        ses.scheduleWithFixedDelay(new CacheCleaner(this), config.getPurgeInterval(), config.getPurgeInterval(), TimeUnit.MILLISECONDS);
        ses.scheduleWithFixedDelay(new CacheMerger(this), config.getMergeInterval(), config.getMergeInterval(), TimeUnit.MILLISECONDS);
        dirtyRatioThreshold = config.getDirtyRatioThreshold();
	}
	

	@Override
	public void put(K key, byte[] value) throws IOException {
		this.put(key, value, -1); // -1 means no time to idle(never expires)
	}

	@Override
	public void put(K key, byte[] value, long tti) throws IOException {
        if (value == null || value.length > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("value is null or too long");
        }
		writeLock(key);
		try {
			Pointer pointer = pointerMap.get(key);
			if (pointer == null) {
				pointer = storageManager.store(value);
			} else {
				pointer = storageManager.update(pointer, value);
			}
			pointer.setTimeToIdle(tti);
            pointer.setLastAccessTime(System.currentTimeMillis());
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
            // the access time can be modified by other readers below.

            if (pointer == null) {
                return null;
            }

            synchronized (pointer) { // pointer may be modified in move thread, use lock here
                if (!pointer.isExpired()) {
                    // access time updated, the following change will not be lost
                    hit.incrementAndGet();
                    pointer.setLastAccessTime(System.currentTimeMillis());
                    return storageManager.retrieve(pointer);
                } else {
                    miss.incrementAndGet();
                    return null;
                }
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

    /**
     * Clear the cache and the underlying storage.
     *
     * Don't do any operation else before clean has finished.
     */
	@Override
	public void clear() {
		this.storageManager.free();
        /**
         * we free storage first, so we can guarantee:
         * 1. entries created/updated before "pointMap" clear process will not be seen. That's what free means.
         * 2. entries created/updated after "pointMap" clear will be safe, as no free operation happens later
         *
         * There is only a small window of inconsistent state between the two free operation, and users should
         * not see this if they behave right.
         */
        this.pointerMap.clear();
	}

	@Override
	public double hitRatio() {
		return 1.0 * hit.get() / (hit.get() + miss.get());
	}

    public long getPurgeCount() {
        return purge.get();
    }

    public long getMoveCount() {
        return move.get();
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

    /**
     * Get the internal lock.
     * @param key
     * @return
     */
    protected ReadWriteLock getLock(K key) {
        return readWriteLock.getLock(Math.abs(key.hashCode()));
    }

	@Override
	public void close() throws IOException {
        this.clear();
        this.ses.shutdownNow();
		this.storageManager.close();
	}
	
	public long count(){
		return pointerMap.size();
	}

    abstract static class CacheDaemonWorker<K> implements Runnable {
        private WeakReference<BigCache<K>> cacheHolder;
        private ScheduledExecutorService ses;

        CacheDaemonWorker(BigCache<K> cache) {
            ses = cache.ses;
            cacheHolder = new WeakReference<BigCache<K>>(cache);
        }

        @Override
        public void run() {
            BigCache cache = cacheHolder.get();
            if (cache == null) {
                // cache is recycled abnormally
                if (ses != null) {
                    ses.shutdownNow();
                    ses = null;
                }
                return;
            }
            try {
                process(cache);
            } catch (IOException e) {
                e.printStackTrace();
            }

            cache.storageManager.clean();
        }

        abstract void process(BigCache<K> cache) throws IOException;
    }

    /**
     * Clean the expired keys.
     *
     * @param <K>
     */
    static class CacheCleaner<K> extends CacheDaemonWorker<K> {
        CacheCleaner(BigCache<K> cache) {
            super(cache);
        }

        @Override
        public void process(BigCache<K> cache) throws IOException {
            Set<K> keys = cache.pointerMap.keySet();

            // store the expired keys according to their associated lock
            Map<ReadWriteLock, List<K>> expiredKeys = new HashMap<ReadWriteLock, List<K>>();

            // find all the keys that may be expired. It's lock less as we will validate later.
            for(K key : keys) {
                Pointer pointer = cache.pointerMap.get(key);
                if (pointer != null && pointer.isExpired()) {
                    ReadWriteLock lock = cache.getLock(key);
                    List<K> keyList = expiredKeys.get(lock);
                    if (keyList == null) {
                        keyList = new ArrayList<K>();
                        expiredKeys.put(lock, keyList);
                    }
                    keyList.add(key);
                }
            }

            // expire keys with write lock, this will complete quickly.
            for (ReadWriteLock lock : expiredKeys.keySet()) {
                List<K> keyList = expiredKeys.get(lock);
                if (keyList == null || keyList.isEmpty()) {
                    continue;
                }

                lock.writeLock().lock();
                try {
                    for(K key : keyList) {
                        Pointer pointer = cache.pointerMap.get(key);
                        if (pointer != null && pointer.isExpired()) {
                            cache.storageManager.removeLight(pointer);
                            cache.pointerMap.remove(key);
                            cache.purge.incrementAndGet();
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }
    }

    static class CacheMerger<K> extends CacheDaemonWorker<K> {
        CacheMerger(BigCache<K> cache) {
            super(cache);
        }

        @Override
        void process(BigCache<K> cache) throws IOException {
            Set<K> keys = cache.pointerMap.keySet();

            // store the keys in dirty block according to the block index
            Map<Integer, List<K>> keysInDirtyBlock = new HashMap<Integer, List<K>>();

            // find all the keys that need to be moved. It's lock less as we will validate later.
            for(K key : keys) {
                Pointer pointer = cache.pointerMap.get(key);
                StorageBlock sb;
                if (pointer != null
                        && ((sb = pointer.getStorageBlock()) != null)
                        && (sb.getDirtyRatio() > cache.dirtyRatioThreshold)) {
                    Integer index = sb.getIndex();
                    List<K> keyList = keysInDirtyBlock.get(index);
                    if (keyList == null) {
                        keyList = new ArrayList<K>();
                        keysInDirtyBlock.put(index, keyList);
                    }
                    keyList.add(key);
                }
            }

            // move keys index by index, we will always work on the block in memory.
            for(List<K> keyList : keysInDirtyBlock.values()) {
                if (keyList == null || keyList.isEmpty()) {
                    continue;
                }
                for(K key : keyList) {
                    cache.readLock(key);
                    try {
                        Pointer pointer = cache.pointerMap.get(key);
                        if (pointer == null) {
                            // not exist now and do nothing, continue with next key;
                            continue;
                        }

                        // pointer is accessed/modified by reader and the merger, use lock here
                        synchronized (pointer) {
                            StorageBlock sb = pointer.getStorageBlock();
                            if (sb.getDirtyRatio() > cache.dirtyRatioThreshold) {
                                byte[] payload = cache.storageManager.remove(pointer);
                                Pointer newPointer = cache.storageManager.storeExcluding(payload, sb);
                                pointer.copyWithoutAccessTime(newPointer);
                                cache.move.incrementAndGet();
                            }
                        }
                    } finally {
                        cache.readUnlock(key);
                    }
                }
            }

        }
    }
}
