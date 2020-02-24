package org.yewc.flink.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.yewc.flink.util.DateUtils;
import org.yewc.flink.util.RichJedisReader;
import org.yewc.flink.util.RichJedisWriter;

/**
 * 详单保存到redis函数，这里是为了做减法操作
 */
public class DetailRedisFunction extends ProcessFunction<Row, Tuple2> {

    /** h key的前缀，可以动态，如field_0 */
    private String hkeyPrefix;

    /** h key的后缀，可以动态，如field_0 目前不支持,直接写死key */
    private String hkeySuffix;

    /** h key的field，如0 */
    private int hkeyField;

    /** h key的value下标，如0,1,2 */
    private int[] hkeyValue;

    /** 详单分割字符 */
    private String splitDetailChar;

    /** 检查field是否已经存在 */
    private boolean checkExists = false;

    /** 打印结果 */
    private boolean print = false;

    /** jedis reader */
    private RichJedisReader jedisReader;

    /** jedis writer */
    private RichJedisWriter jedisWriter;

    private String redisAddress;
    private Integer redisPort;
    private String redisPasswd;
    private Integer redisExpire;

    public static DetailRedisFunction getInstance(String hkeyPrefix, String hkeySuffix,
                                                  int hkeyField, String hkeyValues, String splitDetailChar) {
        return new DetailRedisFunction(hkeyPrefix, hkeySuffix, hkeyField, hkeyValues, splitDetailChar);
    }

    public DetailRedisFunction(String hkeyPrefix, String hkeySuffix,
                               int hkeyField, String hkeyValues, String splitDetailChar) {
        this.hkeyPrefix = hkeyPrefix;
        this.hkeySuffix = hkeySuffix;
        this.hkeyField = hkeyField;
        this.splitDetailChar = splitDetailChar;

        String[] temp = hkeyValues.split(",");
        this.hkeyValue = new int[temp.length];
        for (int i = 0; i < temp.length; i++) {
            this.hkeyValue[i] = Integer.valueOf(temp[i]);
        }
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Tuple2> out) throws Exception {
        // 初始化redis
        if (jedisWriter == null) {
            if (checkExists) {
                jedisReader = new RichJedisReader(redisAddress, redisPort, redisPasswd);
            }
            jedisWriter = new RichJedisWriter(redisAddress, redisPort, redisPasswd, redisExpire);
        }

        // 生成key
        String key = (hkeyPrefix == null ? "" : hkeyPrefix);
        if (StringUtils.isNotBlank(hkeyPrefix)) {
            if (hkeyPrefix.startsWith("field_")) {
                key = row.getField(Integer.valueOf(hkeyPrefix.split("field_")[1])).toString();
            }
        }

        if (StringUtils.isNotBlank(hkeySuffix)) {
            if (hkeySuffix.startsWith("field_")) {
                key += row.getField(Integer.valueOf(hkeySuffix.split("field_")[1])).toString();
            }
        }

        if (StringUtils.isBlank(key)) {
            throw new RuntimeException("the key is empty?! row: " + row.toString());
        }

        String field = row.getField(hkeyField).toString();
        if (StringUtils.isBlank(field)) {
            throw new RuntimeException("the field is empty?! row: " + row.toString());
        }

        if (checkExists) {
            String result = jedisReader.hget(key, field);
            if (StringUtils.isNotBlank(result)) {
                return;
            }
        }

        // 生成value
        String[] data = new String[hkeyValue.length];
        Object temp;
        for (int i = 0; i < hkeyValue.length; i++) {
            temp = row.getField(hkeyValue[i]);
            if (temp == null) {
                temp = "null";
            }
            data[i] = temp.toString();
        }
        String value = String.join(splitDetailChar, data);

        if (StringUtils.isBlank(value)) {
            throw new RuntimeException("the value is empty?! row: " + row.toString());
        }

        // 持久化
        jedisWriter.hset(key, field, value);

        if (print) {
            long curr = System.currentTimeMillis()/1000;

            Row result = new Row(5);
            result.setField(0, DateUtils.formatSimple(curr));
            result.setField(1, DateUtils.format(curr));
            result.setField(2, key);
            result.setField(3, field);
            result.setField(4, value);
            out.collect(new Tuple2(true, result));
        }
    }

    /**
     * redis.address=xx.xx.xx.xx
     * redis.port=9201
     * redis.passwd=xxx
     * data.expire=0
     * @return
     */
    public DetailRedisFunction setRedisUtil(String address, int port, String passwd, int expire, boolean checkExists) {
        this.redisAddress = address;
        this.redisPort = port;
        this.redisPasswd = passwd;
        this.redisExpire = expire;
        this.checkExists = checkExists;
        return this;
    }

    public DetailRedisFunction isPrint(boolean print) {
        this.print = print;
        return this;
    }
}
