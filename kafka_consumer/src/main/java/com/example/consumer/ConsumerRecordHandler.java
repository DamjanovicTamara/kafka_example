package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ConsumerRecordHandler <K, V> {
    void process(ConsumerRecords<K, V> consumerRecords);
}
