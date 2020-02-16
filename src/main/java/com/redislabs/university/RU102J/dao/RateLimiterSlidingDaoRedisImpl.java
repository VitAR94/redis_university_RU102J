package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Date;
import java.util.Random;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = getKey(name);
            long timeMS = new Date().getTime();
            String rnd = String.valueOf(new Random().nextInt(100));
            Transaction t = jedis.multi();
            t.zadd(key, timeMS, String.valueOf(timeMS) + rnd);
            t.zremrangeByScore(key, "-inf", String.valueOf(timeMS - windowSizeMS));
            Response<Long> hits = t.zcard(key);
            t.exec();
            if (hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
    }

    private String getKey(String name) {
        return RedisSchema.getSlidingRateLimiterKey(windowSizeMS, name, maxHits);
    }
}
