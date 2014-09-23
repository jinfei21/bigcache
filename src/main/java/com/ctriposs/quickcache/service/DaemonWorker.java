package com.ctriposs.quickcache.service;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledExecutorService;

import com.ctriposs.quickcache.QuickCache;
import com.ctriposs.quickcache.lock.LockCenter;

public abstract class DaemonWorker<K> implements Runnable {

    private WeakReference<QuickCache> cacheHolder;
    private ScheduledExecutorService scheduler;
    protected LockCenter lockCenter;
    
    public DaemonWorker(QuickCache<K> cache) {
		this.scheduler = cache.getScheduler();
        this.cacheHolder = new WeakReference<QuickCache>(cache);
        this.lockCenter = cache.getLockCenter();
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
        try {
            process(cache);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    

    public abstract void process(QuickCache<K> cache) throws IOException;
    
}
