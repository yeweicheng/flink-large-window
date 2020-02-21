package org.yewc.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RichJedisWriter {

    private static final Logger LOG = LoggerFactory.getLogger(RichJedisWriter.class);

    private final static int RETRIES = 3;
    private int expire;
    private Jedis jedis;
    private String host;
    private int port;
    private String pwd;
    private final int TIMEOUT = 5000;

    public RichJedisWriter(String host, int port, String pwd, int expire) {
        this.host = host;
        this.port = port;
        this.pwd = pwd;
        this.expire = expire;
        this.initInstance();
    }

    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

    public RichJedisWriter set(String key, String value) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        while (!success && retry < RETRIES) {
            try {
                jedis.set(key, value);
                if (this.expire > 0) {
                    jedis.expire(key, expire);
                }
                success = true;
            } catch (JedisConnectionException exception) {
                Thread.sleep(TIMEOUT);
                this.initInstance();
                retry++;
            }
        }
        if (!success) {
            throw new JedisConnectionException("jedis connection error after " + RETRIES + " tries.");
        }
        return this;
    }

    public RichJedisWriter lpush(String key, String... value) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        while (!success && retry < RETRIES) {
            try {
                jedis.lpush(key, value);
                if (this.expire > 0) {
                    jedis.expire(key, expire);
                }
                success = true;
            } catch (JedisConnectionException exception) {
                Thread.sleep(TIMEOUT);
                this.initInstance();
                retry++;
            }
        }
        if (!success) {
            throw new JedisConnectionException("jedis connection error after " + RETRIES + " tries.");
        }
        return this;
    }

    public RichJedisWriter hset(String key, String field, String value) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        while (!success && retry < RETRIES) {
            try {
                jedis.hset(key, field, value);
                if (this.expire > 0) {
                    jedis.expire(key, expire);
                }
                success = true;
            } catch (JedisConnectionException exception) {
                Thread.sleep(TIMEOUT);
                this.initInstance();
                retry++;
            }
        }
        if (!success) {
            throw new JedisConnectionException("jedis connection error after " + RETRIES + " tries.");
        }
        return this;
    }

    public RichJedisWriter expire(String key, int expireTime) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        while (!success && retry < RETRIES) {
            try {
                jedis.expire(key, expireTime);
                success = true;
            } catch (JedisConnectionException exception) {
                Thread.sleep(TIMEOUT);
                this.initInstance();
                retry++;
            }
        }
        if (!success) {
            throw new JedisConnectionException("jedis connection error after " + RETRIES + " tries.");
        }
        return this;
    }

    private void initInstance() {
        jedis = new Jedis(host, port, TIMEOUT);
        jedis.auth(pwd);
    }

}
