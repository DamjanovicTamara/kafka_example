import com.example.producer.KafkaProducerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Scanner;

public class ProducerApplication {
    private static final Logger logger =
            LoggerFactory.getLogger(ProducerApplication.class);

    public static final String TOPIC = "testTopic";

    public static void main(String[] args) throws InterruptedException {
        logger.info("Started Producer Application to send messages");
        Scanner sc= new Scanner(System.in);
        logger.info("Enter number of messages to send ");
        int messageCount= sc.nextInt();
        boolean isAsync = false;

        KafkaProducerProperties producerThread = new KafkaProducerProperties(TOPIC, isAsync,messageCount);
        producerThread.start();

    }
}
