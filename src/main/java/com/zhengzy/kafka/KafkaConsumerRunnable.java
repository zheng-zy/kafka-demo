package com.zhengzy.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>kafka线程消费者</p>
 * Created by @author zhezhiyong@163.com on 2018/11/23.
 */
public class KafkaConsumerRunnable implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;
    private final List<String> topicList;
    private CallbackService callbackService;

    public KafkaConsumerRunnable(KafkaConsumer consumer, List<String> topicList, CallbackService callbackService) {
        this.consumer = consumer;
        this.topicList = topicList;
        this.callbackService = callbackService;
    }

    @Override
    public void run() {
        try {
            if (consumer == null) {
                throw new RuntimeException("kafka消费者不为空");
            }
            if (topicList == null) {
                throw new RuntimeException("订阅kafka主题列表不为空");
            }
            consumer.subscribe(topicList);
            while (!closed.get()) {
                ConsumerRecords records = consumer.poll(1000);
                for (Object partition : records.partitions()) {
                    TopicPartition partition1 = (TopicPartition) partition;
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition1);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ":" + record.value());
                        if (callbackService != null) {
                            callbackService.run(record.offset() + ":" + record.value());
                        }
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    System.out.println("lastOffset = " + lastOffset);
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                    System.out.println("提交 = " + lastOffset);
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
