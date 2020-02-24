package org.yewc.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RichJedisReader {

    private static final Logger LOG = LoggerFactory.getLogger(RichJedisReader.class);

    private Jedis jedis;
    private final static int RETRIES = 3;
    private String host;
    private int port;
    private String pwd;
    private final int TIMEOUT = 5000;

    public RichJedisReader(String host, int port, String pwd) {
        this.host = host;
        this.port = port;
        this.pwd = pwd;
        this.initInstance();
    }

    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }

    public String hget(String key, String field) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        String result = null;
        while (!success && retry < RETRIES) {
            try {
                result = jedis.hget(key, field);
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
        return result;
    }

    public Map<String, String> hgetAll(String key) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        Map<String, String> result = null;
        while (!success && retry < RETRIES) {
            try {
                result = jedis.hgetAll(key);
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
        return result;
    }

    public Set<String> hkeys(String key) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        Set<String> result = null;
        while (!success && retry < RETRIES) {
            try {
                result = jedis.hkeys(key);
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
        return result;
    }

    public List<String> hmget(String key, String... fields) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        List<String> result = null;
        while (!success && retry < RETRIES) {
            try {
                result = jedis.hmget(key, fields);
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
        return result;
    }

    public String get(String key) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        String result = null;
        while (!success && retry < RETRIES) {
            try {
                result = jedis.get(key);
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
        return result;
    }

    public Boolean exists(String key) throws InterruptedException {
        boolean success = false;
        int retry = 0;
        boolean result = false;
        while (!success && retry < RETRIES) {
            try {
                result = jedis.exists(key);
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
        return result;
    }

    private void initInstance() {
        jedis = new Jedis(host, port, TIMEOUT);
        jedis.auth(pwd);
    }

}
