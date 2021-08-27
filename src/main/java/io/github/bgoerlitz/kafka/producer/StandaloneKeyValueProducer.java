package io.github.bgoerlitz.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class StandaloneKeyValueProducer extends AbstractProducer {
    Logger logger = LoggerFactory.getLogger(StandaloneKeyValueProducer.class);

    public StandaloneKeyValueProducer(String propFile) {
        super(propFile);
    }

    @Override
    ProducerRecord generateRecord(Object data) {
        ProducerRecord<String, String> record;
        if (!(data instanceof String)) {
            // TODO: Throw exception
            return null;
        }
        else {
            HashMap<String, String> kv = convertStringToKeyValue((String) data);
            record = new ProducerRecord<String, String>(
                    properties.getProperty("topic"),
                    kv.get("key"),
                    kv.get("value") );
        }
        return record;
    }

    /*
    Expected data is a JSON string with { key: <key>, value: <value> }
     */

    private HashMap<String, String> convertStringToKeyValue(String data) {
        HashMap<String, String> kv = new HashMap<String, String>();
        JSONObject jsonObject = new JSONObject(data);
        String key = jsonObject.getString("key");
        String value = jsonObject.getString("value");
        kv.put("key", key);
        kv.put("value", value);

        return kv;
    }
}
