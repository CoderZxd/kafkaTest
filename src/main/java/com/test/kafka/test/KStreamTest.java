package com.test.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * @author CoderZZ
 * @Title: ${FILE_NAME}
 * @Project: kafkatest
 * @Package com.test.kafka.test
 * @description: TODO:一句话描述信息
 * @Version 1.0
 * @create 2018-08-16 0:07
 **/
public class KStreamTest {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"KStream-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KStreamBuilder builder = new KStreamBuilder();
        //KStream test
//        KStream<String,String> textLine = builder.stream("streams-foo");
        KTable<String,String> textLine = builder.table("streams-foo","KTable-test");
        textLine.print();
        KafkaStreams streams = new KafkaStreams(builder,properties);
        streams.start();
        Thread.sleep(10000L);
        streams.close();
    }
}
