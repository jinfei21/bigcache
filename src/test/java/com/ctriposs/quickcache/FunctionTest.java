package com.ctriposs.quickcache;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.ctriposs.quickcache.CacheConfig.StartMode;
import com.ctriposs.quickcache.CacheConfig.StorageMode;

import junit.framework.TestCase;


public class FunctionTest extends TestCase {
	
	@Before
	public void  setUP() {
		
	}

	public static void main(String args[]) {
		
		try {
			CacheConfig config = new CacheConfig();
			config.setStorageMode(StorageMode.PureFile);
			config.setStartMode(StartMode.File);
			SimpleCache<String> cache = new SimpleCache<String>("D:\\data", config);
			
			String str = "123";

			cache.put("1", "hello1".getBytes());
			cache.put("2", "hello2".getBytes());
			cache.put("3", "hell03".getBytes());
			cache.put("2", "hello4".getBytes());
			
			byte res[] = cache.get("1");
			System.out.println("get1:"+new String(res));
			
			res = cache.delete("1");
			System.out.println("delete1:"+new String(res));
			
			res = cache.get("2");
			System.out.println("get2:"+new String(res));
			
			
			res = cache.get("3");
			System.out.println("get3:"+new String(res));
			
			cache.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
