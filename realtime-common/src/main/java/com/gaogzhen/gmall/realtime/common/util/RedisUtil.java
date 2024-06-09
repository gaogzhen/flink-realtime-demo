package com.gaogzhen.gmall.realtime.common.util;

import com.gaogzhen.gmall.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * @author yhm
 * @create 2024-01-02 15:21
 */
public class RedisUtil {
    private final static JedisPool pool;

    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config, "node2231", 6379, Protocol.DEFAULT_TIMEOUT, "redis@G2ZH");
    }

    public static Jedis getJedis() {
        // Jedis jedis = new Jedis("hadoop102", 6379);

        Jedis jedis = pool.getResource();
        jedis.select(4); // 直接选择 4 号库

        return jedis;
    }

    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();  // 如果 jedis 客户端是 new Jedis()得到的,则是关闭客户端.如果是通过连接池得到的,则归还
        }
    }

    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisURI redisURI = RedisURI.builder()
                .withHost("node2231")
                .withPort(6379)
                .withDatabase(4)
                .withAuthentication("default", "123456")
                .build();
        RedisClient redisClient = RedisClient.create(redisURI);
        return redisClient.connect();
    }


    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }


    public static String getRedisKey(String tableName,String id){
        return Constant.HBASE_NAMESPACE + ":" + tableName + ":" + id;
    }
}
