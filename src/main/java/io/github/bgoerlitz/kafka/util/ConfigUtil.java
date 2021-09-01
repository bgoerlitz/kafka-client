package io.github.bgoerlitz.kafka.util;

import io.github.bgoerlitz.kafka.producer.AbstractProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Enumeration;
import java.util.Properties;

public class ConfigUtil {

    Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

    public static final String PRODUCER_PREFIX = "producer.";
    public static final String CONSUMER_PREFIX = "consumer.";
    public static final String DATAGEN_PREFIX = "datagen.";

    public static final String DATAGEN_PRODUCER_CLASS_CONF = DATAGEN_PREFIX + PRODUCER_PREFIX + "classname";

    public static final String DEFAULT_PROPERTY_FILE = "./config.properties";


    public static Properties getPropertiesFromFile(String propFile)
    {
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream =  new BufferedInputStream(new FileInputStream(propFile));
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new FileNotFoundException(propFile + " not found.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

    public static String getPropertyFromFile(String propFile, String propName)
    {
            Properties properties = getPropertiesFromFile(propFile);
            return properties.getProperty(propName);
    }

    public static Properties getPropertiesWithPrefix(Properties properties, String prefix)
    {
        Properties prefixedProperties = new Properties();
        Enumeration<String> propNames = (Enumeration<String>) properties.propertyNames();
        while (propNames.hasMoreElements()) {
            String propName = propNames.nextElement();
            String propValue = properties.getProperty(propName);

            if (propName.startsWith(prefix)) {
                prefixedProperties.setProperty(propName.substring(prefix.length()), propValue);
            }
        }
        return prefixedProperties;
    }

    public static Properties getDataGenConfig(String propFile)
    {
        Properties properties = getPropertiesFromFile(propFile);
        return getPropertiesWithPrefix(properties, DATAGEN_PREFIX);
    }

    public static Properties getProducerConfig(String propFile)
    {
        Properties properties = getPropertiesFromFile(propFile);
        return getPropertiesWithPrefix(properties, PRODUCER_PREFIX);
    }

    public static Properties getConsumerConfig(String propFile)
    {
        Properties properties = getPropertiesFromFile(propFile);
        return getPropertiesWithPrefix(properties, CONSUMER_PREFIX);
    }

}
