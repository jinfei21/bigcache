package com.ctriposs.quickcache.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockCenter {

	private AtomicBoolean expireActive;
	private AtomicBoolean migrateActive;
	private Map<Integer,ReentrantReadWriteLock> exipreLock;
	private Map<Integer,ReentrantReadWriteLock> migrateLock;
	
	public LockCenter() {
		this.expireActive = new AtomicBoolean(false);
		this.migrateActive = new AtomicBoolean(false);
		this.exipreLock = new ConcurrentHashMap<Integer, ReentrantReadWriteLock>();
		this.migrateLock = new ConcurrentHashMap<Integer, ReentrantReadWriteLock>();
	}
	
	public void activeExpire() {
		this.expireActive.compareAndSet(false, true);
	}
	
	public void releaseExpire() {
		this.expireActive.compareAndSet(true, false);
	}
	
	public void activeMigrate() {
		this.migrateActive.compareAndSet(false, true);
	}
	
	public void releaseMigrate() {
		this.migrateActive.compareAndSet(true, false);
	}
	
	public boolean isNeedLock() {
		return this.expireActive.get()||this.migrateActive.get();
	}
	
	public boolean isExpireActive() {
		return this.expireActive.get();
	}
	
	public boolean isMigrateActive() {
		return this.migrateActive.get();
	}
	
	public ReentrantReadWriteLock getExpireLock(int id) {
		return this.exipreLock.get(id);
	}
	
	public void registerExpireLock(int id,ReentrantReadWriteLock lock) {
		this.exipreLock.put(id, lock);
	}

	public ReentrantReadWriteLock getMigrateLock(int id) {
		return this.migrateLock.get(id);
	}
	
	public void registerMigrateLock(int id,ReentrantReadWriteLock lock) {
		this.migrateLock.put(id, lock);
	}
	
	public void unregisterMigrateLock(int id) {
		this.migrateLock.remove(id);
	}
	
	public void unregisterExpireLock(int id) {
		this.exipreLock.remove(id);
	}
}
