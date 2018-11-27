package com.zhengzy.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * <p></p>
 * Created by zhezhiyong@163.com on 2018/11/27.
 */
public class JavaMain {
    public static void main(String[] args) {
        CallBackServiceImpl callBackService = new CallBackServiceImpl();
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.97.212:9192");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        KafkaConsumerRunnable runnable = new KafkaConsumerRunnable(consumer, Collections.singletonList("test1"), callBackService);
        Thread thread = new Thread(runnable);
        thread.start();
    }
}
