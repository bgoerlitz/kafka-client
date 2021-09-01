package io.github.bgoerlitz.kafka.producer;

import io.github.bgoerlitz.kafka.datagen.AbstractDataGenerator;
import io.github.bgoerlitz.kafka.util.ConfigUtil;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 *
 */

public abstract class AbstractProducer {
    Logger logger = LoggerFactory.getLogger(AbstractProducer.class);

    public static final String TOPIC_NAME_CONF = "topic";

    public static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    Properties properties;
    KafkaProducer<String,String> producer;

    AbstractProducer(String propFile) {

        configure(propFile);
        producer = new KafkaProducer<String, String>(properties);
    }

    abstract ProducerRecord generateRecord(Object data);

    private void configure(String propFile) {
        properties = ConfigUtil.getProducerConfig(propFile);
        logger.info("Using Producer properties");
        //TODO: Make it look better
        for (String k : properties.stringPropertyNames()) {
            logger.info("\t" + k + ": " + properties.getProperty(k));
        }
    }

    public void produceData(Object data) {
        ProducerRecord<String, String> record = generateRecord(data);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    logger.error("Error while producing ",  e);
                }
                else {
                    logger.info("Message meta: Topic=" + recordMetadata.topic()
                            + ", Partition=" + recordMetadata.partition()
                            + ", Offset=" + recordMetadata.offset()
                            + ", Timestamp=" + recordMetadata.timestamp());
                }
            }
        });

//        producer.flush();
    }

    /* Transactional flow is managed in the DataGenerator, so we just
    expose the KafkaProducer transaction APIs.
     */
    public void beginTransaction() {
        producer.beginTransaction();
    }

    public void commitTransaction() {
        producer.commitTransaction();
    }

    public void abortTransaction() {
        producer.abortTransaction();
    }

    public void initTransactions() {
        producer.initTransactions();
    }

    public void close() {
        producer.close();
    }

}
