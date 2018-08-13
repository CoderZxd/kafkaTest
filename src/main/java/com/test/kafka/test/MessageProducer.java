package com.test.kafka.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author CoderZZ
 * @Title: ${FILE_NAME}
 * @Project: kafkatest
 * @Package com.test.kafka.test
 * @description: TODO:一句话描述信息
 * @Version 1.0
 * @create 2018-08-14 0:03
 **/
public class MessageProducer {

    private static final int MSG_NUM = 100000;

    private static final String TOPIC = "test";

    private static final String BROKER_LIST = "localhost:9092";

    private static KafkaProducer<String,String> producer = null;

    static {
        producer = new KafkaProducer<String, String>(initConfig());
    }

    private static Properties initConfig(){
        Properties properties = new Properties();
        //设置kakfa broker列表
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        //设置序列化类
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return properties;
    }

    public static void main(String[] args){
        ProducerRecord<String,String> record = null;
        for(int i = 1;i<=MSG_NUM;i++){
            Message message = new Message();
            message.setMessageInfo("这是第"+i+"条信息!");
            record = new ProducerRecord<String,String>(TOPIC,null,message.getMessageId(),message.toString());
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null){
                        System.out.println("数据发送异常,异常信息为:"+e.getMessage());
                    }
                    System.out.println("==================================Start==================================");
                    System.out.println("partition:"+recordMetadata.partition());
                    System.out.println("offset:"+recordMetadata.offset());
                    System.out.println("topic:"+recordMetadata.topic());
                    System.out.println("checksum:"+recordMetadata.checksum());
                    System.out.println("serializedKeySize:"+recordMetadata.serializedKeySize());
                    System.out.println("serializedValueSize:"+recordMetadata.serializedValueSize());
                    System.out.println("==================================End==================================");
                }
            });
        }
        producer.close();
    }
}
