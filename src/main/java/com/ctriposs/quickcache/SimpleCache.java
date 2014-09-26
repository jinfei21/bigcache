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

public class SimpleCache<K> implements ICache<K> {
	
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

    /** The # of migrate for dirty block recycle. */
    protected AtomicLong migrateCounter = new AtomicLong();
    
    /** The # of migrate for dirty block recycle. */
    protected AtomicLong migrateErrorCounter = new AtomicLong();
	
    /**	The lock manager for key during expire or migrate */
    protected LockCenter lockCenter;
	
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

	
    public SimpleCache(String dir, CacheConfig config) throws IOException {
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
												config.getMaxOffHeapMemorySize(),
												config.getDirtyRatioThreshold());
		
		this.storageManager.loadPointerMap(pointerMap);
		this.scheduler = new ScheduledThreadPoolExecutor(2);
		this.scheduler.scheduleAtFixedRate(new ExpireScheduler(this), config.getExpireInterval(), config.getExpireInterval(), TimeUnit.MILLISECONDS);
		this.scheduler.scheduleAtFixedRate(new MigrateScheduler(this), config.getMigrateInterval(), config.getMigrateInterval(), TimeUnit.MILLISECONDS);
		this.lockCenter = new LockCenter();
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
		Pointer oldPointer = pointerMap.remove(wKey);
		if(oldPointer!=null) {
			byte[] payload = storageManager.remove(oldPointer);
            usedSize.addAndGet((oldPointer.getItemSize()+Meta.META_SIZE) * -1);
			return payload;
		}
		
		return null;
	}
	
	@Override
	public void put(K key, byte[] value) throws IOException {
		put(key, value, Meta.TTL_NEVER_EXPIRE); // -1 means no time to idle(never expires)
	}

	@Override
	public void put(K key, byte[] value, long ttl) throws IOException {
        putCounter.incrementAndGet();
        checkKey(key);
        if (value == null || value.length > MAX_VALUE_LENGTH) {
            throw new IllegalArgumentException("value is null or too long");
        }
        WrapperKey wKey = new WrapperKey(ToBytes(key));
   
		Pointer oldPointer = pointerMap.get(wKey);
			
		if (oldPointer != null) {
			synchronized (oldPointer) {
				Pointer newPointer = storageManager.store(wKey.getKey(),value,ttl);
				if(pointerMap.replace(wKey, oldPointer, newPointer)) {
					storageManager.markDirty(oldPointer);
				}else {
					//因迁移导致获取的不一致
					storageManager.markDirty(oldPointer);
					pointerMap.put(wKey, newPointer);
					usedSize.addAndGet(newPointer.getItemSize()+Meta.META_SIZE);					
				}				
			}
		}else {
			//只能一个线程加新值
			try {
				lockCenter.writeLock(wKey.hashCode());;
				Pointer newPointer = storageManager.store(wKey.getKey(),value,ttl);
				pointerMap.put(wKey, newPointer);
				usedSize.addAndGet(newPointer.getItemSize()+Meta.META_SIZE);					
			}finally {
				lockCenter.writeUnlock(wKey.hashCode());
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

	    private WeakReference<SimpleCache> cacheHolder;
	    private ScheduledExecutorService scheduler;
	    
	    public DaemonWorker(SimpleCache<K> cache) {
			this.scheduler = cache.scheduler;
	        this.cacheHolder = new WeakReference<SimpleCache>(cache);
	    }
		
	    @Override
	    public void run() {
	    	SimpleCache cache = cacheHolder.get();
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
	    
	    public abstract void process(SimpleCache<K> cache);	    
	}
	
	class MigrateScheduler<K> extends DaemonWorker<K> {
	    
	    public MigrateScheduler(SimpleCache<K> cache) {
			super(cache);
		}

		@Override
		public void process(SimpleCache<K> cache) {
			
			migrateCounter.incrementAndGet();
			for(IBlock block:cache.storageManager.getDirtyBlocks()) {
				try {
					for(Meta meta : block.getAllValidMeta()) {
						Item item = block.readItem(meta);
						WrapperKey wKey = new WrapperKey(item.getKey());
						Pointer oldPointer = pointerMap.get(wKey);
						if(oldPointer != null) {							
							Pointer newPointer = storageManager.store(item.getKey(), item.getValue(), meta.getTtl());							
							if(pointerMap.replace(wKey, oldPointer, newPointer)) {
								storageManager.markDirty(oldPointer);								
							}else {
								storageManager.markDirty(newPointer);
							}
						}
					}
					block.free();
				}catch(Throwable t) {
					migrateErrorCounter.incrementAndGet();
				}
			}
			storageManager.clean();
		}
	}
	
	class ExpireScheduler<K> extends DaemonWorker<K> {
 
		public ExpireScheduler(SimpleCache<K> cache) {
			super(cache);
		}

		@Override
		public void process(SimpleCache<K> cache) {
			expireCounter.incrementAndGet();

			for (WrapperKey wKey : pointerMap.keySet()) {
				Pointer oldPointer = pointerMap.get(wKey);
				if (oldPointer != null) {
					if (oldPointer.isExpired()) {
						if (pointerMap.remove(wKey, oldPointer)) {							
							int size = storageManager.markDirty(oldPointer);
							usedSize.addAndGet(-1*size);
						}
					}
				}
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
