package com.example.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoCallback implements Callback {
    private static final Logger logger = LoggerFactory.getLogger(DemoCallback.class);
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallback(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            logger.info("Message key:{},message value:{} is sent to partition:{}, offset :{}, in :{} ms",key,message,metadata.partition(),metadata.offset(),elapsedTime);
        } else {
            exception.printStackTrace();
        }
    }
}
