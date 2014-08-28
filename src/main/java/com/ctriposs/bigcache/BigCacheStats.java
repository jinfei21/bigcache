package com.ctriposs.bigcache;

/**
 * Created by wenlu on 2014/8/21.
 */
public class BigCacheStats {
    private final long hitCount;
    private final long missCount;

    private final long getCount;
    private final long putCount;
    private final long deleteCount;

    private final long expireCount;
    private final long moveCount;

    private final long size;
    private final long count;

    public BigCacheStats(long hitCount, long missCount, long getCount, long putCount, long deleteCount, long expireCount,
                         long moveCount, long count, long size) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.getCount = getCount;
        this.putCount = putCount;
        this.deleteCount = deleteCount;
        this.expireCount = expireCount;
        this.moveCount = moveCount;
        this.count = count;
        this.size = size;
    }

    public BigCacheStats() {
        this(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public BigCacheStats minus(BigCacheStats other) {
        if (other == null)
            return this;

        return new BigCacheStats(
                this.hitCount - other.hitCount,
                this.missCount - other.missCount,
                this.getCount - other.getCount,
                this.putCount - other.putCount,
                this.deleteCount - other.deleteCount,
                this.expireCount - other.expireCount,
                this.moveCount - other.moveCount,
                this.count - other.count,
                this.size - other.size
        );
    }

    public BigCacheStats plus(BigCacheStats other) {
        if (other == null)
            return this;

        return new BigCacheStats(
                this.hitCount + other.hitCount,
                this.missCount + other.missCount,
                this.getCount + other.getCount,
                this.putCount + other.putCount,
                this.deleteCount + other.deleteCount,
                this.expireCount + other.expireCount,
                this.moveCount + other.moveCount,
                this.count - other.count,
                this.size + other.size
        );
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public long getGetCount() {
        return getCount;
    }

    public long getPutCount() {
        return putCount;
    }

    public long getDeleteCount() {
        return deleteCount;
    }

    public long getExpireCount() {
        return expireCount;
    }

    public long getMoveCount() {
        return moveCount;
    }

    public long getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }
}
