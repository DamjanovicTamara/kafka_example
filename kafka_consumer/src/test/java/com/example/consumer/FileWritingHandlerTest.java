package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class FileWritingHandlerTest {

    private final static String TEST_CONFIG_FILE = "/consumertest.properties";

    @Test
    public void fileWritten_WhenGivenPathAndMessagesAreConsumed() throws IOException {
        final Properties testConsumerProps = KafkaConsumerSample.loadProperties(TEST_CONFIG_FILE);
        final String testFilePath = testConsumerProps.getProperty("file.path");
        try {
            final ConsumerRecordHandler<String, String> recordsHandler = new FileWritingRecordHandler(Paths.get(testFilePath));
            recordsHandler.process(createConsumerRecords());
            final List<String> expectedWords = Arrays.asList("what we", "think we", "become");
            List<String> actualRecords = Files.readAllLines(Paths.get(testFilePath));
            assertThat(actualRecords, equalTo(expectedWords));
        } finally {
            Files.deleteIfExists(Paths.get(testFilePath));
        }
    }

    private ConsumerRecords<String, String> createConsumerRecords() {
        final String topic = "test";
        final int partition = 0;
        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        final List<ConsumerRecord<String, String>> consumerRecordsList = new ArrayList<>();
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, null, "what we"));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, null, "think we"));
        consumerRecordsList.add(new ConsumerRecord<>(topic, partition, 0, null, "become"));
        final Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        recordsMap.put(topicPartition, consumerRecordsList);

        return new ConsumerRecords<>(recordsMap);
    }
}
