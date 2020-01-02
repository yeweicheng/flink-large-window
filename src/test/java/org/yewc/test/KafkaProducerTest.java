package org.yewc.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.16.6.191:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(properties);
        String topic = "test2";

        String[] abc = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
        Random random = new Random();

        int nowSize = 0;
        Long preKey = null;
        Set<String> allCount = new HashSet<>();
        Map<Long, Set<String>> keyCount = new HashMap<>();
        while (true) {
            Thread.sleep(1000);
            int value = random.nextInt(10);
            String c1 = random.nextInt(10000) + "";
            Long current = System.currentTimeMillis();

            JSONObject jo = new JSONObject();
//            jo.put("c1", abc[random.nextInt(26)]);
            jo.put("c1", c1);
            jo.put("c2", value);
            jo.put("c3", Long.valueOf(value));
            jo.put("c4", current);
            String data = jo.toString();
            producer.send(new ProducerRecord<String, String>(topic, data));
//            System.out.println(data);

            Long currentKey = DateUtils.parse(DateUtils.format(System.currentTimeMillis()/1000).substring(0, 16) + ":59");
            if (nowSize == 0) {
                preKey = currentKey;
            }
            if (!keyCount.containsKey(currentKey)) {
                keyCount.put(currentKey, new HashSet<>());
            }
            keyCount.get(currentKey).add(c1);
            if (keyCount.size() > nowSize) {
                if (nowSize > 0) {
                    keyCount.get(preKey).removeAll(allCount);
                    System.out.println(DateUtils.format(preKey/1000)
                            + " -> " + keyCount.get(preKey).size()
                            + " -> " + (keyCount.get(preKey).size() + allCount.size()));
                    allCount.addAll(keyCount.get(preKey));
                }
                nowSize = keyCount.size();
                preKey = currentKey;
            }
        }

    }

}
