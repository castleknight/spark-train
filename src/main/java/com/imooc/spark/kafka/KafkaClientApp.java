package com.imooc.spark.kafka;

public class KafkaClientApp {
    public static void main(String[] args) throws InterruptedException {
        new KafkaProducer(KafkaProperties.TOPIC).start();
        Thread.sleep(10000);
        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
