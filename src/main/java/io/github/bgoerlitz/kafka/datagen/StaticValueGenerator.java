package io.github.bgoerlitz.kafka.datagen;

import io.github.bgoerlitz.kafka.producer.AbstractProducer;
import io.github.bgoerlitz.kafka.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Properties;

public class StaticValueGenerator extends AbstractDataGenerator {
    Logger logger = LoggerFactory.getLogger(StaticValueGenerator.class);

    private static final String DATA_VALUE_CONF = "data-value";
    private static final String DELAY_CONF = "delay";
    private static final String DEFAULT_DATA_VALUE = "static value";
    private static final String DEFAULT_DELAY = "0";

    String dataValue;
    int delay;

    public StaticValueGenerator(String propFile)
    {
        super(propFile);
    }

    void configure(String propFile)
    {
        super.configure(propFile);

        dataValue = (String) properties.getOrDefault(DATA_VALUE_CONF, DEFAULT_DATA_VALUE);
        delay = Integer.parseInt((String) properties.getOrDefault(DELAY_CONF, DEFAULT_DELAY));

        createProducer(propFile);

    }

    @Override
    public void generateAndSendData() {
        int i = 0;
        boolean running = true;
        while (running) {
            producer.produceData(dataValue);

            try
            {
                Thread.sleep(delay);
            }
            catch(InterruptedException ex)
            {
                Thread.currentThread().interrupt();
            }

            i++;
            running = (recordLimit <= 0 || i < recordLimit);
        }
    }

    public static void main(String[] args) {
        AbstractDataGenerator dg = new StaticValueGenerator(ConfigUtil.DEFAULT_PROPERTY_FILE);
        dg.generateAndSendData();
    }
}
