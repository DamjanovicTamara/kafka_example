package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class ConsumerApplicationTest {

    private final static String TEST_CONFIG_FILE = "/consumertest.properties";
    private  MockConsumer<String, String> mockConsumer;
    private  KafkaConsumerSample consumerApplication;
    private Throwable pollException;
    private String topic;
    private Properties testConsumerProps;
    private String testFilePath;
    private TopicPartition topicPartition;

    @BeforeEach
    public void onBefore() throws IOException {
        testConsumerProps = KafkaConsumerSample.loadProperties(TEST_CONFIG_FILE);
        topic = testConsumerProps.getProperty("input.topic.name");
        testFilePath = testConsumerProps.getProperty("file.path");
        final ConsumerRecordHandler<String, String> recordsHandler = new FileWritingRecordHandler(Paths.get(testFilePath));
        topicPartition = new TopicPartition(topic, 0);
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumerApplication= new KafkaConsumerSample(mockConsumer, recordsHandler, ex -> this.pollException = ex);
    }
    @Test
    public void consumeMessagesFromTheTopic() throws Exception {
        //Given
        mockConsumer.schedulePollTask(() -> addTopicPartitionsAssignmentAndAddConsumerRecords(topic, mockConsumer, topicPartition));
        mockConsumer.schedulePollTask(consumerApplication::shutdown);
        consumerApplication.consumeMessages(testConsumerProps);
        //When
        final List<String> expectedWords = Arrays.asList("foo", "bar", "baz");
        List<String> actualRecords = Files.readAllLines(Paths.get(testFilePath));
        //Then
        assertThat(actualRecords, equalTo(expectedWords));
        //Fix and add this !
        Assertions.assertThat(actualRecords).isNotNull();
      //  Assertions.assertThat(actualRecords.key()).isNull();
    }

    private void addTopicPartitionsAssignmentAndAddConsumerRecords(final String topic,
                                                                   final MockConsumer<String, String> mockConsumer,
                                                                   final TopicPartition topicPartition) {

        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        mockConsumer.rebalance(Collections.singletonList(topicPartition));
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        addConsumerRecords(mockConsumer,topic);
    }

    private void addConsumerRecords(final MockConsumer<String, String> mockConsumer, final String topic) {
        mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0, null, "foo"));
        mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 1, null, "bar"));
        mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 2, null, "baz"));
    }


    @Test
    void exceptionIsHandledCorrectly_WhenSubscribeToTopicAndExceptionOccurs() {
        // GIVEN
        mockConsumer.schedulePollTask(() -> mockConsumer.setPollException(new KafkaException("poll exception")));
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());//.());//stop());

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        startOffsets.put(tp, 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);

        // WHEN
       consumerApplication.consumeMessages(testConsumerProps);

        // THEN
        assertThat(pollException).isInstanceOf(KafkaException.class).hasMessage("poll exception");
        assertThat(mockConsumer.closed()).isTrue();
    }

}
