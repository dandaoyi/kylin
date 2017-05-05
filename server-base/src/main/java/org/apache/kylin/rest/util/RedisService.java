package org.apache.kylin.rest.util;

import com.google.gson.Gson;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

@Service
public class RedisService {
    private static final Logger logger = LoggerFactory.getLogger(RedisService.class);

    @Resource
    private ShardedJedisPool shardedJedisPool;

    private final int EXPIRE = 60 * 60;

    public void set(String key, Object val) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPool.getResource();
            key = getKey(key);
            jedis.set(key, new Gson().toJson(val));
            jedis.expire(key, EXPIRE);
        } catch (Exception e){
            logger.error("", e);
        } finally {
            returnJedis(jedis);
        }
    }

    public static void returnJedis(ShardedJedis jedis) {
        if (jedis != null)
            jedis.close();
    }

    public void set(String key, Object val, int expireTime) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPool.getResource();
            key = getKey(key);
            jedis.set(key, new Gson().toJson(val));
            jedis.expire(key, expireTime);
        } catch (Exception e){
            logger.error("", e);
        } finally {
            returnJedis(jedis);
        }
    }

    public void delete(String key) {
        ShardedJedis jedis = null;
        try {
            key = getKey(key);
            jedis = shardedJedisPool.getResource();
            jedis.del(key);
        } catch (Exception e){
            logger.error("", e);
        } finally {
            returnJedis(jedis);
        }

    }

    public <T> T get(String key, Class<T> type) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPool.getResource();
            key = getKey(key);
            String val = jedis.get(key);
            return new Gson().fromJson(val, type);
        } catch (Exception e){
            logger.error("", e);
        } finally {
            returnJedis(jedis);
        }
        return null;
    }

    public void setExpireTime(String key, int expireTime) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPool.getResource();
            key = getKey(key);
            jedis.expire(key, expireTime);
        } catch (Exception e){
            logger.error("", e);
        } finally {
            returnJedis(jedis);
        }
    }

    private String getKey(String key) {
        return MD5Utils.getMD5Key(key);
    }

}