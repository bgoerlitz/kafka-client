package io.github.bgoerlitz.kafka.producer;

import io.github.bgoerlitz.kafka.util.ConfigUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneValueProducer extends AbstractProducer {
    Logger logger = LoggerFactory.getLogger(StandaloneValueProducer.class);

    public StandaloneValueProducer(String propFile) {
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
            record = new ProducerRecord<String, String>(properties.getProperty("topic"), (String) data);
        }
        return record;
    }
}
