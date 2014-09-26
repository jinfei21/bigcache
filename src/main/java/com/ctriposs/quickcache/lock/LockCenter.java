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
	private ReentrantReadWriteLock[] locks;
	
	public LockCenter(int storagePower) {
		this.expireActive = new AtomicBoolean(false);
		this.migrateActive = new AtomicBoolean(false);
		this.exipreLock = new ConcurrentHashMap<Integer, ReentrantReadWriteLock>();
		this.migrateLock = new ConcurrentHashMap<Integer, ReentrantReadWriteLock>();
		if (!(storagePower >= 1 && storagePower <= 11)) {
			throw new IllegalArgumentException("storage power must be in {1..11}");
		}

		int lockSize = (int) Math.pow(2, storagePower);
		locks = new ReentrantReadWriteLock[lockSize];
		for (int i = 0; i < locks.length; i++){
			locks[i] = new ReentrantReadWriteLock();
		}
	}
	
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
	
	/**
	 * Locks lock associated with given id.
	 * 
	 * @param id value, from which lock is derived
	 */
	public void readLock(int id) {
		getLock(id).readLock().lock();
	}

	/**
	 * Unlocks lock associated with given id.
	 * 
	 * @param id value, from which lock is derived
	 */
	public void readUnlock(int id) {
		getLock(id).readLock().unlock();
	}

	/**
	 * Locks lock associated with given id.
	 * 
	 * @param id value, from which lock is derived
	 */
	public void writeLock(int id) {
		getLock(id).writeLock().lock();
	}

	/**
	 * Unlocks lock associated with given id.
	 * 
	 * @param id value, from which lock is derived
	 */
	public void writeUnlock(int id) {
		getLock(id).writeLock().unlock();
	}

    /**
     * Locks all locks as write lock.
     */
    public void writeLockForAll() {
        for (int i = 0; i < locks.length; i++) {
            getLock(i).writeLock().lock();
        }
    }

    /**
     * Unlocks all locks as write lock.
     */
    public void writeUnlockForAll() {
        for (int i = 0; i < locks.length; i++) {
            getLock(i).writeLock().unlock();
        }
    }
    
	/**
	 * Finds the lock associated with the id
	 * 
	 * @param id value, from which lock is derived
	 * @return lock which is associated with the id
	 */
	public ReentrantReadWriteLock getLock(int id) {
		// locks.length-1 is a string of ones since lock.length is power of 2,
		// thus ending cancels out the higher bits of id and leaves the lower bits
		// to determine the lock.
		return locks[id & (locks.length - 1)];
	}
}
