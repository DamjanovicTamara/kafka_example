import com.example.consumer.ConsumerRecordHandler;
import com.example.consumer.FileWritingRecordHandler;

import com.example.consumer.KafkaConsumerSample;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;


public class ConsumerApplication {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerApplication.class.getName());

    public static void main(String[] args) throws IOException {

        final Properties consumerAppProps = KafkaConsumerSample.loadProperties("/consumer.properties");
        logger.info("Properties are loaded");

        final String filePath = consumerAppProps.getProperty("file.path");
        //Create consumer from the properties
        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerAppProps);
        //Write a file
        final ConsumerRecordHandler<String, String> recordsHandler = new FileWritingRecordHandler(Paths.get(filePath));
        //Create consumer application
        final KafkaConsumerSample consumerApplication = new KafkaConsumerSample(recordsHandler, consumer);
        Runtime.getRuntime().addShutdownHook(new Thread(consumerApplication::shutdown));
        //Subscribe to the topic
        consumerApplication.consumeMessages(consumerAppProps);

    }

}
