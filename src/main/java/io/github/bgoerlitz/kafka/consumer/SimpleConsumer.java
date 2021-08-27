package io.github.bgoerlitz.kafka.consumer;

import io.github.bgoerlitz.kafka.datagen.AbstractDataGenerator;
import io.github.bgoerlitz.kafka.datagen.StaticKeyValueGenerator;
import io.github.bgoerlitz.kafka.util.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
    Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    public static final String TOPIC_NAME_CONF = "topic";
    public static final String POLL_TIMEOUT_CONF = "poll-timeout";

    public static final String DEFAULT_POLL_TIMEOUT = "1000";

    Properties properties;
    KafkaConsumer<String, String> consumer;
    Duration pollTimeout;

    public SimpleConsumer(String propFile) {
        configure(propFile);

        consumer = new KafkaConsumer<String, String>(properties);
    }

    public void consume() {
        consumer.subscribe(Arrays.asList(properties.getProperty(TOPIC_NAME_CONF)));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout);

            if (records.isEmpty()) {
                break;
            }

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Message Meta: Topic=" + record.topic() + ", Partition=" + record.partition()
                        + ", Offset=" + record.offset() + ", Timestamp=" + record.timestamp()
                        + ". Message Data: Key=" + record.key() + ", Value=" + record.value());
                System.out.println(record.value());
            }
        }

        consumer.close();
    }

    private void configure(String propFile) {
        properties = ConfigUtil.getConsumerConfig(propFile);

        logger.info("Using Consumer properties");
        //TODO: Make it look better
        for (String k : properties.stringPropertyNames()) {
            logger.info("\t" + k + ": " + properties.getProperty(k));
        }

        pollTimeout = Duration.ofMillis(Long.parseLong(
                (String) properties.getOrDefault(POLL_TIMEOUT_CONF, DEFAULT_POLL_TIMEOUT)));

    }

    public static void main(String[] args) {
        SimpleConsumer consumer = new SimpleConsumer(ConfigUtil.DEFAULT_PROPERTY_FILE);
        consumer.consume();
    }
}
