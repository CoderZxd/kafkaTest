package com.test.kafka.test;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author CoderZZ
 * @Title: ${FILE_NAME}
 * @Project: kafkatest
 * @Package com.test.kafka.test
 * @description: TODO:一句话描述信息
 * @Version 1.0
 * @create 2018-08-14 23:41
 **/
public class MyPartitioner implements Partitioner{

    private static final Integer PARTITIONS = 5;
    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(value == null){
            return 0;
        }
        String keyString = String.valueOf(key);
        int partition = Integer.parseInt(keyString) % PARTITIONS;
        System.out.println("key:"+keyString+",partition:"+partition);
        return partition;
    }

    /**
     * This is called when partitioner is closed.
     */
    @Override
    public void close() {

    }

    /**
     * Configure this class with the given key-value pairs
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
