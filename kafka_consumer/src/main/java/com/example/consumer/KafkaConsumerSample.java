package com.example.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class KafkaConsumerSample {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerSample.class);

    private volatile boolean keepConsuming = true;
    private final ConsumerRecordHandler<String, String> recordsHandler;
    private final Consumer<String, String> consumer;
    private final java.util.function.Consumer<Throwable> exceptionConsumer;
    private Throwable pollException;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);


    public KafkaConsumerSample(final Consumer<String, String> consumer,
                               final ConsumerRecordHandler<String, String> recordsHandler, java.util.function.Consumer<Throwable> exceptionConsumer) {
        this.consumer = consumer;
        this.recordsHandler = recordsHandler;
        this.exceptionConsumer = exceptionConsumer;
    }

    public KafkaConsumerSample(ConsumerRecordHandler<String, String> recordsHandler, Consumer<String, String> consumer) {
        this.recordsHandler = recordsHandler;
        this.consumer = consumer;
        exceptionConsumer=ex -> this.pollException=ex;
    }

    public void consumeMessages(final Properties consumerProps) {
        try {
            consumer.subscribe(Collections.singletonList(consumerProps.getProperty("input.topic.name")));
            while (keepConsuming) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(60));
                if(consumerRecords.isEmpty()) {
                    logger.info("Consumer : {}, Found no records", consumerProps.getProperty("group.id"));
                    keepConsuming=false;
                    continue;
                }
                recordsHandler.process(consumerRecords);
                consumerRecords.forEach(record->{
                    logger.info("Received message, message key: {},message value: {},at offset: {}" , record.key() , record.value() ,record.offset());

                });
            }
        } catch (WakeupException e) {
            logger.info("Shutting down...");
        } catch (RuntimeException ex) {
            exceptionConsumer.accept(ex);
        } finally {
            close();
            logger.info("The consumer is now gracefully closed.");
        }
    }
    private void close(){
        consumer.close();
        try {
            countDownLatch.await(60,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Error while closing consumer");
        }
    }

    public void shutdown()  {
        keepConsuming = false;
        logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
        consumer.wakeup();
        countDownLatch.countDown();
    }

    public static Properties loadProperties(String fileName) throws IOException {
        final Properties props = new Properties();
        InputStream input = KafkaConsumerSample.class.getResourceAsStream(fileName);
        props.load(input);
        try{
            Objects.requireNonNull(input).close();
        }catch (NullPointerException e){
            throw new IOException("Properties file is empty, invalid or does not exist");
        }
        return props;
    }
}
