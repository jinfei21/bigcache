package com.ctriposs.quickcache.utils;

public class HashUtil {

    public static int JSHash(byte[] bytes) {
    
        int hash = 1315423911;

        for (int i = 0; i < bytes.length; i++) {
            hash ^= ((hash << 5) + bytes[i] + (hash >> 2));
        }

        return (hash & 0x7FFFFFFF);
    }
    
    
}
