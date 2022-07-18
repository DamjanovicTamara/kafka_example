package com.example.config;

public interface KafkaConfig {
    String KAFKA_BROKERS = "localhost:9092";

    Integer MESSAGE_COUNT=1000;

    String CLIENT_ID="client1";

    String TOPIC_NAME="input-topic";

    String GROUP_ID_CONFIG="consumerGroup10";

    Integer MAX_NO_MESSAGE_FOUND_COUNT=100;

    String OFFSET_RESET_LATEST="latest";

    String OFFSET_RESET_EARLIER="earliest";

    Integer MAX_POLL_RECORDS=1;
}
