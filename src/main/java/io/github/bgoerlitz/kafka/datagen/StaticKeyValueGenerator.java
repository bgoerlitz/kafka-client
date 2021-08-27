package io.github.bgoerlitz.kafka.datagen;

import io.github.bgoerlitz.kafka.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticKeyValueGenerator extends AbstractDataGenerator {
    Logger logger = LoggerFactory.getLogger(StaticKeyValueGenerator.class);

    private static final String DATA_VALUE_CONF = "data-value";
    private static final String DATA_KEY_CONF = "data-key";
    private static final String DELAY_CONF = "delay";

    private static final String DEFAULT_DATA_KEY = "static key";
    private static final String DEFAULT_DATA_VALUE = "static value";
    private static final String DEFAULT_DELAY = "0";

    String dataValue;
    String dataKey;
    String data;
    int delay;

    public StaticKeyValueGenerator(String propFile)
    {
        super(propFile);
    }

    void configure(String propFile)
    {
        super.configure(propFile);

        dataValue = (String) properties.getOrDefault(DATA_VALUE_CONF, DEFAULT_DATA_VALUE);
        dataKey = (String) properties.getOrDefault(DATA_KEY_CONF, DEFAULT_DATA_KEY);
        delay = Integer.parseInt((String) properties.getOrDefault(DELAY_CONF, DEFAULT_DELAY));

        data = "{ key: " + dataKey + ", value: " + dataValue + "}";

        createProducer(propFile);

    }

    @Override
    public void generateAndSendData() {
        int i = 0;
        boolean running = true;
        while (running) {
            producer.produceData(data);

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
        AbstractDataGenerator dg = new StaticKeyValueGenerator(ConfigUtil.DEFAULT_PROPERTY_FILE);
        dg.generateAndSendData();
    }
}
