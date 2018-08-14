package com.test.kafka.test;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * @author CoderZZ
 * @Title: ${FILE_NAME}
 * @Project: kafkatest
 * @Package com.test.kafka.test
 * @description: TODO:一句话描述信息
 * @Version 1.0
 * @create 2018-08-14 1:02
 **/
public class MessageConsumer {

    private static  final String TOPIC = "test";

    private static final String BROKER_LIST = "localhost:9092";

    private  static Properties properties = new Properties();

    static {
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"test");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,1024);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    }

    public static void main(String[] args){
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {

            //在消费者平衡操作之前、消费者停止拉取消息之后被调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync();//同步提交偏移量
            }

            //平衡之后、消费者开始拉取消息之前被调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                long committedOffset = -1;
                for(TopicPartition partition:partitions){
                    //获取该分区已消费的偏移量
                    committedOffset = consumer.committed(partition).offset();
                    //重置偏移量到上一次提交的偏移量下一个位置开始消费
                    consumer.seek(partition,committedOffset+1);
                }
            }
        });

        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(1000);
                for(ConsumerRecord<String,String> record:records){
                    System.out.println("record key:"+record.key()+",partition:"+record.partition()+",record value:"+record.value());
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if(exception == null){
                            System.out.println("提交成功!");
                        }else{
                            System.out.println("提交异常:"+ exception.getMessage());
                        }
                    }
                });
            }
        }catch (Exception e){
            consumer.close();
        }
    }
}
