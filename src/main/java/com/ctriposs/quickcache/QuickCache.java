package com.ctriposs.quickcache;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ctriposs.quickcache.lock.LockCenter;
import com.ctriposs.quickcache.storage.Item;
import com.ctriposs.quickcache.storage.Meta;
import com.ctriposs.quickcache.storage.Pointer;
import com.ctriposs.quickcache.storage.StorageManager;
import com.ctriposs.quickcache.storage.WrapperKey;
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

    /** The # of expire due to expiration. */
    protected AtomicLong expireCounter = new AtomicLong();
    
    /** The # of expire due to expiration. */
    protected AtomicLong expireErrorCounter = new AtomicLong();

    /** The # of migrate for dirty block recycle. */
    protected AtomicLong migrateCounter = new AtomicLong();
    
    /** The # of migrate for dirty block recycle. */
    protected AtomicLong migrateErrorCounter = new AtomicLong();
    
    /**	The lock manager for key during expire or migrate */
    protected LockCenter lockCenter = new LockCenter();
    
    /** The thread pool for expire and migrate*/
    private ScheduledExecutorService scheduler;
    
	/** The directory to store cached data */
	private String cacheDir;
    
    /** The total storage size we have used, including the expired ones which are still in the pointermap */
    protected AtomicLong usedSize = new AtomicLong();

	/** The internal map. */
	protected final ConcurrentMap<WrapperKey, Pointer> pointerMap = new ConcurrentHashMap<WrapperKey, Pointer>();
    
	/** Managing the storages. */
	private final StorageManager storageManager;
	
	/** global lock. */
	private Object gLock = new Object();
    
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
												config.getMaxOffHeapMemorySize(),config.getDirtyRatioThreshold());
		
		this.scheduler = new ScheduledThreadPoolExecutor(2);
		this.scheduler.scheduleAtFixedRate(new ExpireScheduler(this), config.getExpireInterval(), config.getExpireInterval(), TimeUnit.MILLISECONDS);
		this.scheduler.scheduleAtFixedRate(new MigrateScheduler(this), config.getMigrateInterval(), config.getMigrateInterval(), TimeUnit.MILLISECONDS);
		
    	
    }

    private void checkKey(K key) {
    	if(key == null) {
    		throw new IllegalArgumentException("key is null");
    	}
    }
	@Override
	public byte[] get(K key) throws IOException {
		getCounter.incrementAndGet();
		checkKey(key);
		WrapperKey wKey = new WrapperKey(ToBytes(key));
		Pointer pointer = pointerMap.get(wKey);
		if (pointer == null) {
			missCounter.incrementAndGet();
			return null;
		}

		if (!pointer.isExpired()) {
			// access time updated, the following change will not be lost
			pointer.setLastAccessTime(System.currentTimeMillis());
			hitCounter.incrementAndGet();
			return storageManager.retrieve(pointer);
		} else {
			missCounter.incrementAndGet();
			return null;
		}

	}

	@Override
	public byte[] delete(K key) throws IOException {
		deleteCounter.incrementAndGet();
		checkKey(key);
        WrapperKey wKey = new WrapperKey(ToBytes(key));
		ReentrantReadWriteLock expireLock = null;
		ReentrantReadWriteLock migrateLock = null;
		if(lockCenter.isNeedLock()) {
			expireLock = lockCenter.getExpireLock(wKey.hashCode());
			migrateLock = lockCenter.getMigrateLock(wKey.hashCode());
		}
		
		try {
			
			if(expireLock != null) {
				expireLock.readLock().lock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().lock();
			}
			Pointer oldPointer = pointerMap.get(wKey);
            if (oldPointer != null) {
            	synchronized (oldPointer) {
            		Pointer checkPointer = pointerMap.get(wKey);
            		if(oldPointer==checkPointer) {
	            		pointerMap.remove(wKey);
						byte[] payload = storageManager.remove(oldPointer);
		                usedSize.addAndGet(payload.length * -1);
						return payload;
            		}else if(checkPointer !=null) {
	            		pointerMap.remove(wKey);
						byte[] payload = storageManager.remove(checkPointer);
		                usedSize.addAndGet(payload.length * -1);
						return payload;
            		}
				}
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
        checkKey(key);
        if (value == null || value.length > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("value is null or too long");
        }
        
        WrapperKey wKey = new WrapperKey(ToBytes(key));
        
		ReentrantReadWriteLock expireLock = null;
		ReentrantReadWriteLock migrateLock = null;
		if(lockCenter.isNeedLock()) {
			expireLock = lockCenter.getExpireLock(wKey.hashCode());
			migrateLock = lockCenter.getMigrateLock(wKey.hashCode());
		}
		
		try {
			if(expireLock != null) {
				expireLock.readLock().lock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().lock();
			}
			Pointer oldPointer = pointerMap.get(wKey);
			
			if (oldPointer != null) {
                // update and get the new storage
				synchronized (oldPointer) {
					Pointer checkPointer = pointerMap.get(wKey);
					if(oldPointer==checkPointer||checkPointer !=null) {
						int size = storageManager.markDirty(checkPointer);
						usedSize.addAndGet(size * -1);
						Pointer pointer = storageManager.store(wKey.getKey(),value,ttl);
						pointerMap.put(wKey, pointer);						
					}
				}

			}else {

				oldPointer = pointerMap.get(wKey);
				if(oldPointer == null) {
					Pointer	pointer = storageManager.store(wKey.getKey(),value,ttl);
					pointerMap.put(wKey, pointer);	
				}else {
					synchronized (oldPointer) {
						int size = storageManager.markDirty(oldPointer);
						usedSize.addAndGet(size * -1);
						Pointer pointer = storageManager.store(wKey.getKey(),value,ttl);
						pointerMap.put(wKey, pointer);		
					}
				}
			}
			usedSize.addAndGet((wKey.getKey().length+4+value.length) * -1);
		}finally {
			if(expireLock != null) {
				expireLock.readLock().unlock();
			}
			if(migrateLock != null) {
				migrateLock.readLock().unlock();
			}
		}

	}
	
	private byte[] ToBytes(K key) throws IOException {
		if(key instanceof byte[]) {
			return (byte[])key;
		} else if(key instanceof String){
			return ((String) key).getBytes();
		}else {
		
			try {
				ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); 
				ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
				objectOut.writeObject(key);
				return byteOut.toByteArray();
			} catch (IOException e) {
				throw e;
			}
		}
	}

	@Override
	public boolean contains(K key) {
		return pointerMap.containsKey(key);
	}

	@Override
	public void clear() {

        pointerMap.clear();
		storageManager.free();
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
    
	abstract class DaemonWorker<K> implements Runnable {

	    private WeakReference<QuickCache> cacheHolder;
	    private ScheduledExecutorService scheduler;
	    protected LockCenter lockCenter;
	    
	    public DaemonWorker(QuickCache<K> cache) {
			this.scheduler = cache.scheduler;
	        this.cacheHolder = new WeakReference<QuickCache>(cache);
	    }
		
	    @Override
	    public void run() {
	    	QuickCache cache = cacheHolder.get();
	        if (cache == null) {
	            // cache is recycled abnormally
	            if (scheduler != null) {
	            	scheduler.shutdownNow();
	                scheduler = null;
	            }
	            return;
	        }
	        process(cache);

	    }
	    

	    public abstract void process(QuickCache<K> cache);
	    
	}
	
	class MigrateScheduler<K> extends DaemonWorker<K> {

		    
		    public MigrateScheduler(QuickCache<K> cache) {
				super(cache);
			}

			@Override
			public void process(QuickCache<K> cache) {

				migrateCounter.incrementAndGet();
				for(IBlock block:cache.storageManager.getDirtyBlocks()) {
					try {
						cache.lockCenter.activeMigrate();
						for(Meta meta : block.getAllValidMeta()) {
							Item item = block.readItem(meta);
							ReentrantReadWriteLock lock = null;
							WrapperKey wKey = new WrapperKey(item.getKey());
							try {
								
								lock = new ReentrantReadWriteLock();
								lock.writeLock().lock();
								cache.lockCenter.registerMigrateLock(wKey.hashCode(), lock);
								Pointer oldPointer = cache.pointerMap.get(wKey);
								if(oldPointer != null) {
									synchronized (oldPointer) {
										oldPointer = cache.pointerMap.get(wKey);
										if(oldPointer!=null) {//client may delete key
											if(oldPointer.getLastAccessTime()==meta.getLastAccessTime()) {
												
												storageManager.markDirty(oldPointer);
												Pointer pointer = storageManager.store(wKey.getKey(),item.getValue(),meta.getTtl());
												cache.pointerMap.put(wKey, pointer);
											}
										}
									}
								}
							}finally {
								lock.writeLock().unlock();
								cache.lockCenter.unregisterMigrateLock(wKey.hashCode());							
							}
						
						}
						block.free();
					}catch(Throwable t) {
						migrateErrorCounter.incrementAndGet();
					}finally {
						cache.lockCenter.releaseMigrate();						
					}
				}

		        cache.storageManager.clean();				
			}
	}
	
	class ExpireScheduler<K> extends DaemonWorker<K> {

	    
		public ExpireScheduler(QuickCache<K> cache) {
			super(cache);
		}

		@Override
		public void process(QuickCache<K> cache)  {
			expireCounter.incrementAndGet();
			for(WrapperKey wKey:cache.pointerMap.keySet()) {
				cache.lockCenter.activeExpire();
				Pointer pointer = cache.pointerMap.get(wKey);
				if(pointer != null&&pointer.isExpired()) {
					ReentrantReadWriteLock lock = null;
					try {
						lock = new ReentrantReadWriteLock();
						lock.writeLock().lock();
						cache.lockCenter.registerMigrateLock(wKey.hashCode(), lock);
						synchronized (pointer) {
							pointer = cache.pointerMap.get(wKey);
							if(pointer != null) {
								if(pointer.isExpired()) {
									try {
										cache.pointerMap.remove(wKey);
										byte[] payload = storageManager.remove(pointer);
						                usedSize.addAndGet(payload.length * -1);
									}catch(Throwable t) {
										expireErrorCounter.incrementAndGet();
									}
								}
							}
						}
					}finally {
						lock.writeLock().unlock();
						cache.lockCenter.unregisterExpireLock(wKey.hashCode());
					}
				}
				cache.lockCenter.releaseExpire();
			}
			
		}
		
	}

	public long getHitCounter() {
		return hitCounter.get();
	}

	public long getMissCounter() {
		return missCounter.get();
	}

	public long getGetCounter() {
		return getCounter.get();
	}

	public long getPutCounter() {
		return putCounter.get();
	}

	public long getDeleteCounter() {
		return deleteCounter.get();
	}

	public long getExpireCounter() {
		return expireCounter.get();
	}

	public long getExpireErrorCounter() {
		return expireErrorCounter.get();
	}

	public long getMigrateCounter() {
		return migrateCounter.get();
	}

	public long getMigrateErrorCounter() {
		return migrateErrorCounter.get();
	}

	public long getUsedSize() {
		return usedSize.get();
	}

}
