package io.github.bgoerlitz.kafka.datagen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalDataGenerator extends AbstractDataGenerator{
    Logger logger = LoggerFactory.getLogger(TransactionalDataGenerator.class);

    private static final String TRANSACTION_COUNT_CONF = "transaction-count";
    private static final String RECORDS_PER_TRANSACTION_CONF = "records-per-transaction";

    private static final String DEFAULT_TRANSACTION_COUNT = "1";
    private static final String DEFAULT_RECORDS_PER_TRANSACTION = "1";

    int numTransactions;
    int recordsPerTransaction;



    public TransactionalDataGenerator(String propFile) {
        super(propFile);
    }

    void configure(String propFile) {
        super.configure(propFile);

        numTransactions = Integer.parseInt((String) properties.getOrDefault(TRANSACTION_COUNT_CONF, DEFAULT_TRANSACTION_COUNT));
        recordsPerTransaction = Integer.parseInt((String) properties.getOrDefault(RECORDS_PER_TRANSACTION_CONF, DEFAULT_RECORDS_PER_TRANSACTION));

        createProducer(propFile);
        producer.initTransactions();
    }

    @Override
    public void generateAndSendData() {
        for (int t = 0; t < numTransactions; t++) {
            try{
                producer.beginTransaction();
                for (int r = 0; r < recordsPerTransaction; r++) {
                    producer.produceData("tx" + t + "r" + r);
                }
                producer.commitTransaction();
            } catch (Exception e) {
                logger.error("Error in transaction ",  e);
                producer.abortTransaction();
                producer.close();
            }
        }
        producer.close();
    }
}
