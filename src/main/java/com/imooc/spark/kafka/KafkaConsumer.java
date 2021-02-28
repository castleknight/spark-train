package com.imooc.spark.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * This is a KafkaConsumer
 */

public class KafkaConsumer extends Thread{

       private String topic;

       private Consumer<Integer,String> consumer;

    public KafkaConsumer(String topic){
        this.topic=topic;

        Properties properties=new Properties();

        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        properties.put("group.id",KafkaProperties.GROUP_ID);

        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<Integer,String>(properties);

    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topic));


        while(true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));

            Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();

            while (iterator.hasNext()) {
                String message = iterator.next().value();
                System.out.println("receive: " + message);
            }
        }
    }
}
