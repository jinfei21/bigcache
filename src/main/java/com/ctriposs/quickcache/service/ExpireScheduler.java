package com.ctriposs.quickcache.service;

import java.io.IOException;

import com.ctriposs.quickcache.QuickCache;



public class ExpireScheduler<K> extends DaemonWorker<K> {

    
	public ExpireScheduler(QuickCache<K> cache) {
		super(cache);
	}

	@Override
	public void process(QuickCache<K> cache) throws IOException {


		
		
	}
	

}
