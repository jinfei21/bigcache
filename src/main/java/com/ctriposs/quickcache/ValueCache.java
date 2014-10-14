package com.ctriposs.quickcache;

import static com.ctriposs.quickcache.utils.ByteUtil.ToBytes;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.ctriposs.quickcache.CacheConfig.StartMode;
import com.ctriposs.quickcache.storage.Item;
import com.ctriposs.quickcache.storage.Meta;
import com.ctriposs.quickcache.storage.Pointer;
import com.ctriposs.quickcache.storage.StorageManager;
import com.ctriposs.quickcache.storage.WrapperKey;
import com.ctriposs.quickcache.utils.FileUtil;

public class ValueCache<K> implements ICache<K> {
	
	/** The default storage block cleaning period which is 10 minutes. */
	public static final long DEFAULT_MIGRATE_INTERVAL = 1 * 60 * 1000;
	
	/** The default purge interval which is 10 minutes. */
	public static final long DEFAULT_EXPIRE_INTERVAL = 10 * 60 * 1000;

	/** The default storage block cleaning threshold. */
	public static final float DEFAULT_STORAGE_BLOCK_CLEANING_THRESHOLD = 0.5f;

    /** The default threshold for dirty block recycling */
    public static final double DEFAULT_DIRTY_RATIO_THRESHOLD = 0.5;

    /** The length of value can't be greater than 4m */
    public static final int MAX_VALUE_LENGTH = 4 * 1024 * 1024;

	/** The hit counter. */
    private AtomicLong hitCounter = new AtomicLong();

	/** The miss counter. */
	private AtomicLong missCounter = new AtomicLong();

    /** The get counter. */
	private AtomicLong getCounter = new AtomicLong();

    /** The put counter. */
    private AtomicLong putCounter = new AtomicLong();

    /** The delete counter. */
    private AtomicLong deleteCounter = new AtomicLong();

    /** The # of expire due to expiration. */
    private AtomicLong expireCounter = new AtomicLong();

    /** The # of migrate for dirty block recycle. */
    private AtomicLong migrateCounter = new AtomicLong();
    
    /** The # of migrate for dirty block recycle. */
    private AtomicLong migrateErrorCounter = new AtomicLong();
    
    /** The # of expire for dirty block recycle. */
    private AtomicLong expireErrorCounter = new AtomicLong();
	
    /** The thread pool for expire and migrate*/
    private ScheduledExecutorService scheduler;

    /** The total storage size we have used, including the expired ones which are still in the pointer map */
	private AtomicLong usedSize = new AtomicLong();

	/** The internal map. */
    private final ConcurrentMap<K, Pointer> pointerMap = new ConcurrentHashMap<K, Pointer>(16, 0.75f, 1024);
    
	/** Managing the storages. */
	private final StorageManager storageManager;

	
    public ValueCache(String dir, CacheConfig config) throws IOException {
        String cacheDir = dir;
		if (!cacheDir.endsWith(File.separator)) {
			cacheDir += File.separator;
		}
		// validate directory
		if (!FileUtil.isFilenameValid(cacheDir)) {
			throw new IllegalArgumentException("Invalid cache data directory : " + cacheDir);
		}
		
		this.storageManager = new StorageManager(cacheDir,
                config.getCapacityPerBlock(),
                config.getInitialNumberOfBlocks(),
                config.getStorageMode(),
                config.getMaxOffHeapMemorySize(),
                config.getDirtyRatioThreshold(),
                config.getStartMode());

		this.scheduler = new ScheduledThreadPoolExecutor(2);
		this.scheduler.scheduleWithFixedDelay(new ExpireScheduler(this), config.getExpireInterval(), config.getExpireInterval(), TimeUnit.MILLISECONDS);
		this.scheduler.scheduleWithFixedDelay(new MigrateScheduler(this), config.getMigrateInterval(), config.getMigrateInterval(), TimeUnit.MILLISECONDS);
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
       
		Pointer oldPointer = pointerMap.remove(key);
		if(oldPointer!=null) {
			//byte[] payload = storageManager.remove(oldPointer);
			byte[] payload = storageManager.retrieve(oldPointer);
			byte[] bytes = new byte[1];
			Pointer newPointer = storageManager.store(null,bytes,Meta.TTL_DELETE);
			storageManager.markDirty(oldPointer);
			storageManager.markDirty(newPointer);
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
       
		Pointer newPointer = storageManager.store(null, value, ttl);
		while(true) {
			Pointer oldPointer = pointerMap.get(key);
			if(oldPointer != null){
				if(oldPointer.getCreateNanoTime()<=newPointer.getCreateNanoTime()) {

					if(pointerMap.replace(key, oldPointer, newPointer)) {
						storageManager.markDirty(oldPointer); 
						break;
					}
				}else {
					storageManager.markDirty(newPointer);
					break;
				}
			} else {
				Pointer checkPointer = pointerMap.putIfAbsent(key, newPointer);
				if (checkPointer != null) {
					if (checkPointer.getCreateNanoTime() >= newPointer.getCreateNanoTime()) {
						storageManager.markDirty(newPointer);
						break;
					}
				} else {
					usedSize.addAndGet(newPointer.getItemSize() + Meta.META_SIZE);
					break;
				}
			}
		}

	}

	@Override
	public boolean contains(K key) throws IOException {
		
		WrapperKey wKey = new WrapperKey(ToBytes(key));
		return pointerMap.containsKey(wKey);
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
    
	abstract static class DaemonWorker<K> implements Runnable {

	    private WeakReference<ValueCache> cacheHolder;
	    private ScheduledExecutorService scheduler;
	    
	    public DaemonWorker(ValueCache<K> cache) {
			this.scheduler = cache.scheduler;
	        this.cacheHolder = new WeakReference<ValueCache>(cache);
	    }
		
	    @Override
	    public void run() {
	    	ValueCache cache = cacheHolder.get();
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
	    
	    public abstract void process(ValueCache<K> cache);	    
	}
	
	class MigrateScheduler<K> extends DaemonWorker<K> {
	    
	    public MigrateScheduler(ValueCache<K> cache) {
			super(cache);
		}

		@Override
		public void process(ValueCache<K> cache) {
			
			migrateCounter.incrementAndGet();
			Set<IBlock> dirtyBlocks = cache.storageManager.getDirtyBlocks();
			for (K key : cache.pointerMap.keySet()) {
				Pointer oldPointer = pointerMap.get(key);
				if(oldPointer != null) {	
					if(dirtyBlocks.contains(oldPointer.getBlock())) {
						try {
							Pointer newPointer = storageManager.store(null, null, 0L);							
							if(cache.pointerMap.replace(key, oldPointer, newPointer)) {
								storageManager.markDirty(oldPointer);								
							}else {
								storageManager.markDirty(newPointer);
							}
						}catch(Throwable t) {
							migrateErrorCounter.incrementAndGet();
						}
					}
				}
				
			}
						
			storageManager.clean();
		}
	}
	
	class ExpireScheduler<K> extends DaemonWorker<K> {
 
		public ExpireScheduler(ValueCache<K> cache) {
			super(cache);
		}

		@Override
		public void process(ValueCache<K> cache) {
			expireCounter.incrementAndGet();

			for (K key : cache.pointerMap.keySet()) {
				Pointer oldPointer = pointerMap.get(key);
				if (oldPointer != null) {
					if (oldPointer.isExpired()) {
						try {
							if (pointerMap.remove(key, oldPointer)) {							
								int size = storageManager.markDirty(oldPointer);
								usedSize.addAndGet(-1*size);
							}
						}catch(Throwable t) {
							expireErrorCounter.incrementAndGet();
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
