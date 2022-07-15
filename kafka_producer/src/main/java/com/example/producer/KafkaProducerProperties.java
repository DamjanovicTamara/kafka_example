package com.example.producer;


import com.example.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaProducerProperties  extends Thread{

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerProperties.class);
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleProducer";

    private static KafkaProducer<Integer, String> producer;
    private final String topic;
    private final int sendMessageCount;
    Properties properties = loadProperties();
    private  Properties loadProperties() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    public KafkaProducerProperties(String topic,int sendMessageCount){
        this.topic = topic;
        this.sendMessageCount = sendMessageCount;
        producer = new KafkaProducer<Integer, String>(properties);
    }

    static void runProducer(final int sendMessageCount) throws InterruptedException {
        int time = (int) System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);
        try {
            for (int index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Integer, String> record =
                        new ProducerRecord<>(KafkaConfig.TOPIC_NAME, index, "Hello this is a new message" + index);
                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if (metadata != null) {
                        logger.info("Message key:{},message value:{} is sent to partition:{}, offset :{}, in :{} ms",record.key(),record.value(),metadata.partition(),metadata.offset(),elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
            }
            countDownLatch.await(60, TimeUnit.SECONDS);
        } finally {
            producer.flush();
            producer.close();
        }

    }
    public void run() {
        try {
            runProducer(sendMessageCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
