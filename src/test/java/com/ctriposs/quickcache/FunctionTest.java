package com.ctriposs.quickcache;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.ctriposs.quickcache.CacheConfig.StorageMode;

import junit.framework.TestCase;


public class FunctionTest extends TestCase{
	
	@Before
	public void  setUP() {
		
	}
	
	@Test
	public void testPut() {
		
		
		
	}

	@Test
	public void testDelete() {
		
		
		
	}
	
	
	@Test
	public void testGet() {
		
		
		
	}
	
	
	public static void main(String args[]) {
		
		try {
			CacheConfig config = new CacheConfig();
			config.setStorageMode(StorageMode.PureFile);
			SimpleCache<String> cache = new SimpleCache<String>("D:\\data", config);
			
			String str = "123";
			byte data[] = str.getBytes("utf-8");
			cache.put("1", data);
			cache.put("1", data);
			
			byte res[] = cache.get("1");
			System.out.println(new String(res));
			
			
			cache.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
