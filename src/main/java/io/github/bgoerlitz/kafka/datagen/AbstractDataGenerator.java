package io.github.bgoerlitz.kafka.datagen;

import io.github.bgoerlitz.kafka.producer.AbstractProducer;
import io.github.bgoerlitz.kafka.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Properties;

public abstract class AbstractDataGenerator {
    Logger logger = LoggerFactory.getLogger(AbstractDataGenerator.class);

    private static final String PRODUCER_CLASSNAME_CONF = ConfigUtil.PRODUCER_PREFIX + "classname";
    private static final String RECORD_LIMIT_CONF = "record-limit";
    private static final String DEFAULT_RECORD_LIMIT = "-1";

    Properties properties;
    AbstractProducer producer;

    int recordLimit;


    AbstractDataGenerator(String propFile)
    {
        properties = new Properties();
        configure(propFile);
    }

    void configure(String propFile)
    {
        properties = ConfigUtil.getDataGenConfig(propFile);

        logger.info("Using DataGen properties");
        //TODO: Make it look better
        for (String k : properties.stringPropertyNames()) {
            logger.info("\t" + k + ": " + properties.getProperty(k));
        }

        recordLimit = Integer.parseInt((String) properties.getOrDefault(RECORD_LIMIT_CONF, DEFAULT_RECORD_LIMIT));
        int recordLimit;

    }

    void createProducer(String propFile)
    {
        String producerClassname = properties.getProperty(PRODUCER_CLASSNAME_CONF);
        try {
            Constructor producerCtor = Class.forName(producerClassname).getConstructor(String.class);
            producer = (AbstractProducer) producerCtor.newInstance(propFile);
        } catch (Exception e) {
            logger.error("Error while creating DataGenerator: ", e);
        }
    }


    public abstract void generateAndSendData();
}
