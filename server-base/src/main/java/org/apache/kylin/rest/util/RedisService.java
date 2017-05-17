/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.kylin.rest.util;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

@Service
public class RedisService {
    private static final Logger logger = LoggerFactory.getLogger(RedisService.class);

    @Resource
    private ShardedJedisPool shardedJedisPool;

    // 缓存6个小时。 单位是秒 。 后续移到配置文件
    private final int EXPIRE = 6 * 60 * 60;

    public void set(String key, Object val) {
        ShardedJedis jedis = null;
        try {
            jedis = shardedJedisPool.getResource();
            key = getKey(key);
            jedis.set(key, new Gson().toJson(val));
            jedis.expire(key, EXPIRE);
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            returnJedis(jedis);
        }
    }

    private String getKey(String key) {
        return MD5Utils.getMD5Key(key);
    }

}