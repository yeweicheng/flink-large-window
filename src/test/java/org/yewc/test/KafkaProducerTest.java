package org.yewc.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

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

        while (true) {
            Thread.sleep(100);
            JSONObject jo = new JSONObject();
            jo.put("c1", abc[random.nextInt(26)]);
            jo.put("c2", random.nextInt(10000));
            jo.put("c3", System.currentTimeMillis());
            String data = jo.toString();
            producer.send(new ProducerRecord<String, String>(topic, data));
//            System.out.println(data);
        }

    }

}
