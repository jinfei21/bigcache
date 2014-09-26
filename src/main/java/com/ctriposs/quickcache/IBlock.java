package com.ctriposs.quickcache;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.ctriposs.quickcache.storage.Item;
import com.ctriposs.quickcache.storage.Meta;
import com.ctriposs.quickcache.storage.Pointer;

public interface IBlock extends Comparable<IBlock>, Closeable {
	  /**
     * Retrieves the payload associated with the pointer and always update the access time.
     *
     * @param pointer the pointer
     * @return the byte[]
     * @throws IOException
     */
    byte[] retrieve(Pointer pointer) throws IOException;

	
	/**
	 * Stores the payload.
	 *
	 * @param payload the payload
	 * @return the pointer
	 * @throws IOException
	 */
	Pointer store(byte[] key,byte[] value,long ttl) throws IOException;
	
	/**
	 * Removes the payload and marks the used space as dirty.
	 *
	 * @param pointer the pointer
	 * @return the byte[]
	 * @throws IOException
	 */
	byte[] remove(Pointer pointer) throws IOException; 
	
	/**
	 *  Marks exSpace as dirty.
	 * 
	 * @param pointer the pointer
	 * @return space size for pointer
	 */
	int markDirty(Pointer pointer);
	
	/**
	 * Get all valid meta of this storage block
	 * 
	 * @return count
	 */
	List<Meta> getAllValidMeta()throws IOException; 
	
	
	/**
	 * Get item for this meta in this storage block
	 * 
	 * @return item
	 */
	Item readItem(Meta meta)throws IOException; 
	
	/**
	 * Calculates and returns total size of the dirty space.
	 *
	 * @return the total size of the dirty space.
	 */
	long getDirty();
	
	/**
	 * Calculates and returns total size of the used space.
	 * 
	 * @return the total size of the used space.
	 */
	long getUsed();
	
	/**
	 * Calculates and returns total capacity of the block.
	 *
	 * @return the total capacity of the block.
	 */
	long getCapacity();
	
	/**
	 * Calculates and returns the dirty to capacity ratio
	 *  
	 * @return dirty ratio
	 */
	double getDirtyRatio();
	
	/**
	 * Get the index of this storage block
	 * 
	 * @return an index
	 */
	int getIndex();
	
	/**
	 * Frees the storage.
	 */
	void free();
	
	/**
	 * Get meta count of this storage block
	 * 
	 * @return count
	 */
	int getMetaCount();
	
	/**
	 * Active the storage.
	 */
	void active();
	
	/**
	 * Used the storage.
	 */
	void used();
}
