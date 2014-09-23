package com.ctriposs.quickcache;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ctriposs.quickcache.lock.LockCenter;
import com.ctriposs.quickcache.service.ExpireScheduler;
import com.ctriposs.quickcache.service.MigrateScheduler;
import com.ctriposs.quickcache.storage.Pointer;
import com.ctriposs.quickcache.storage.StorageManager;
import com.ctriposs.quickcache.utils.FileUtil;


public class QuickCache<K> implements ICache<K> {
	
	/** The default storage block cleaning period which is 10 minutes. */
	public static final long DEFAULT_MIGRATE_INTERVAL = 10 * 60 * 1000;
	
	/** The default purge interval which is 10 minutes. */
	public static final long DEFAULT_EXPIRE_INTERVAL = 10 * 60 * 1000;

	/** The default storage block cleaning threshold. */
	public static final float DEFAULT_STORAGE_BLOCK_CLEANING_THRESHOLD = 0.5f;

    /** The default threshold for dirty block recycling */
    public static final double DEFAULT_DIRTY_RATIO_THRESHOLD = 0.5;

    /** The length of value can't be greater than 4m */
    public static final int MAX_VALUE_LENGTH = 4 * 1024 * 1024;

	/** The hit counter. */
	protected AtomicLong hitCounter = new AtomicLong();

	/** The miss counter. */
	protected AtomicLong missCounter = new AtomicLong();

    /** The get counter. */
    protected AtomicLong getCounter = new AtomicLong();

    /** The put counter. */
    protected AtomicLong putCounter = new AtomicLong();

    /** The delete counter. */
    protected AtomicLong deleteCounter = new AtomicLong();

    /** The # of purges due to expiration. */
    protected AtomicLong expireCounter = new AtomicLong();

    /** The # of moves for dirty block recycle. */
    protected AtomicLong migrateCounter = new AtomicLong();
    
    /**	The lock manager for key during expire or migrate */
    protected LockCenter lockCenter = new LockCenter();
    
    /** The thread pool for expire and migrate*/
    private ScheduledExecutorService scheduler;
    
	/** The directory to store cached data */
	private String cacheDir;
    
	/** dirty ratio which controls block recycle */
    private final double dirtyRatioThreshold;
    
    /** The total storage size we have used, including the expired ones which are still in the pointermap */
    protected AtomicLong usedSize = new AtomicLong();

	/** The internal map. */
	protected final ConcurrentMap<K, Pointer> pointerMap = new ConcurrentHashMap<K, Pointer>();
    
	/** Managing the storages. */
	private final StorageManager storageManager;
    
    public QuickCache(String dir, CacheConfig config) throws IOException {
    	this.cacheDir = dir;
		if (!this.cacheDir.endsWith(File.separator)) {
			this.cacheDir += File.separator;
		}
		// validate directory
		if (!FileUtil.isFilenameValid(this.cacheDir)) {
			throw new IllegalArgumentException("Invalid cache data directory : " + this.cacheDir);
		}
		
		this.storageManager = new StorageManager(this.cacheDir, 
												config.getCapacityPerBlock(),
												config.getInitialNumberOfBlocks(), 
												config.getStorageMode(), 
												config.getMaxOffHeapMemorySize());
		
		this.scheduler = new ScheduledThreadPoolExecutor(2);
		this.scheduler.scheduleAtFixedRate(new ExpireScheduler(this), config.getExpireInterval(), config.getExpireInterval(), TimeUnit.MILLISECONDS);
		this.scheduler.scheduleAtFixedRate(new MigrateScheduler(this), config.getMigrateInterval(), config.getMigrateInterval(), TimeUnit.MILLISECONDS);
		
		this.dirtyRatioThreshold = config.getDirtyRatioThreshold();
    	
    }

	@Override
	public byte[] get(K key) throws IOException {
		getCounter.incrementAndGet();
		
		ReentrantReadWriteLock expireLock = null;
		ReentrantReadWriteLock migrateLock = null;
		if(lockCenter.isNeedLock()) {
			expireLock = lockCenter.getExpireLock(key.hashCode());
			migrateLock = lockCenter.getMigrateLock(key.hashCode());
		}
		
		try {
			if(expireLock != null) {
				expireLock.readLock().lock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().lock();
			}
			Pointer pointer = pointerMap.get(key);
            if (pointer == null) {
                missCounter.incrementAndGet();
                return null;
            }

            if (!pointer.isExpired()) {
                // access time updated, the following change will not be lost
            	pointer.setCreateTime(System.currentTimeMillis());
                hitCounter.incrementAndGet();
                return storageManager.retrieve(pointer);
            } else {
                missCounter.incrementAndGet();
                return null;
            }
            
		}finally {
			if(expireLock != null) {
				expireLock.readLock().unlock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().unlock();
			}
		}
	}
	
	@Override
	public byte[] delete(K key) throws IOException {
		deleteCounter.incrementAndGet();
		ReentrantReadWriteLock expireLock = null;
		ReentrantReadWriteLock migrateLock = null;
		if(lockCenter.isNeedLock()) {
			expireLock = lockCenter.getExpireLock(key.hashCode());
			migrateLock = lockCenter.getMigrateLock(key.hashCode());
		}
		
		try {
			
			if(expireLock != null) {
				expireLock.readLock().lock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().lock();
			}
			Pointer pointer = pointerMap.get(key);
            if (pointer == null) {
				byte[] payload = storageManager.remove(pointer);
				pointerMap.remove(key);
                usedSize.addAndGet(payload.length * -1);
				return payload;
            }
			
		}finally {
			if(expireLock != null) {
				expireLock.readLock().unlock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().unlock();
			}
		}
		
		
		return null;
	}


	@Override
	public void put(K key, byte[] value) throws IOException {
		put(key, value, -1); // -1 means no time to idle(never expires)
	}

	@Override
	public void put(K key, byte[] value, long ttl) throws IOException {
        putCounter.incrementAndGet();
        if (value == null || value.length > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("value is null or too long");
        }
        
		ReentrantReadWriteLock expireLock = null;
		ReentrantReadWriteLock migrateLock = null;
		if(lockCenter.isNeedLock()) {
			expireLock = lockCenter.getExpireLock(key.hashCode());
			migrateLock = lockCenter.getMigrateLock(key.hashCode());
		}
		
		try {
			if(expireLock != null) {
				expireLock.readLock().lock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().lock();
			}
			Pointer pointer = pointerMap.get(key);
			
			if (pointer != null) {
                // update and get the new storage
				int size = storageManager.markDirty(pointer);
                usedSize.addAndGet(size * -1);
			}
			
			pointer = storageManager.store(key,value,ttl);
			pointerMap.put(key, pointer);
		}finally {
			if(expireLock != null) {
				expireLock.readLock().unlock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().unlock();
			}
		}

	}
	
	private byte[] formatToBytes(K key) {
		
	}

	@Override
	public boolean contains(K key) {
		return pointerMap.containsKey(key);
	}

	@Override
	public void clear() {
		storageManager.free();
        /**
         * we free storage first, so we can guarantee:
         * 1. entries created/updated before "pointMap" clear process will not be seen. That's what free means.
         * 2. entries created/updated after "pointMap" clear will be safe, as no free operation happens later
         *
         * There is only a small window of inconsistent state between the two free operation, and users should
         * not see this if they behave right.
         */
        pointerMap.clear();
        usedSize.set(0);
	}

	@Override
	public void close() throws IOException {
        clear();
        scheduler.shutdownNow();
		storageManager.close();
	}
	
	@Override
	public double hitRatio() {
		return 1.0 * hitCounter.get() / (hitCounter.get() + missCounter.get());
	}
    
	public ScheduledExecutorService getScheduler() {
		return this.scheduler;
	}
	
	public LockCenter getLockCenter() {
		return this.lockCenter;
	}
	
	public StorageManager getStorageManager() {
		return this.storageManager;
	}
	
	public void incMigrateCount() {
		this.migrateCounter.incrementAndGet();
	}
	
	public void incExpireCount() {
		this.expireCounter.incrementAndGet();
	}
}
