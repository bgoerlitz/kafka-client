package io.github.bgoerlitz.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedValueProducer extends AbstractProducer {
    Logger logger = LoggerFactory.getLogger(DistributedValueProducer.class);

    private final static String PARTITION_CONF = "partition";
    private final static String DEFAULT_PARTITION = "0";

    public DistributedValueProducer(String propFile) {
        super(propFile);
    }

    @Override
    ProducerRecord generateRecord(Object data) {
        ProducerRecord<String, String> record;
        if (!(data instanceof String)) {
            // TODO: Throw exception
            return null;
        }
        else{
            record = new ProducerRecord<String, String>(
                    properties.getProperty(TOPIC_NAME_CONF),
                    Integer.parseInt((String) properties.getOrDefault(PARTITION_CONF, DEFAULT_PARTITION)),
                    "key",
                    (String) data );
        }
        return record;
    }
}
