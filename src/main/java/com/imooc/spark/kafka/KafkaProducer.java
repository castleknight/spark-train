package com.imooc.spark.kafka;


import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class KafkaProducer extends Thread {

    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic){
        this.topic=topic;

        Properties properties=new Properties();


        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        properties.put("request.required.acks","1");

        producer=new org.apache.kafka.clients.producer.KafkaProducer<Integer, String>(properties);


    }


    @Override
    public void run(){
         int messageNo=1;

         while(true){
             String message="message_"+messageNo;
             producer.send(new ProducerRecord<Integer, String>(topic,message));
             System.out.println("Sent: "+message);

             messageNo++;

             try{
                 Thread.sleep(2000);
             }catch (Exception e){
                 e.printStackTrace();
             }
         }

    }

}
