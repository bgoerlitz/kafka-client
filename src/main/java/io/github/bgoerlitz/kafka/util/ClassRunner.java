package io.github.bgoerlitz.kafka.util;

import io.github.bgoerlitz.kafka.consumer.SimpleConsumer;
import io.github.bgoerlitz.kafka.datagen.AbstractDataGenerator;
import io.github.bgoerlitz.kafka.producer.AbstractProducer;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class ClassRunner {
    Logger logger = LoggerFactory.getLogger(AbstractProducer.class);

    public static void main(String[] args) {
        Options options = new Options();

        Option generator = Option.builder("generator")
                .hasArg()
                .desc("The classname of the DataGenerator. Cannot be used with consumer.")
                .build();
        Option consumer = Option.builder("consumer")
                .desc("Run as consumer. Cannot be used with generator.")
                .build();
        Option properties = Option.builder("properties")
                .hasArg()
                .desc("The path to the properties file.")
                .build();
        options.addOption(generator);
        options.addOption(consumer);
        options.addOption(properties);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse( options, args );

            if (line.hasOption("generator") && line.hasOption("consumer")) {
                System.err.println( "Cannot run both generator and consumer in the same process.");
                System.exit(1);
            }

            String propFile = line.hasOption("properties") ?
                    line.getOptionValue("properties") :
                    ConfigUtil.DEFAULT_PROPERTY_FILE;

            if (line.hasOption("generator")) {
                try {
                    Constructor dgCtor = Class.forName(line.getOptionValue("generator")).getConstructor(String.class);
                    AbstractDataGenerator dg = (AbstractDataGenerator) dgCtor.newInstance(propFile);
                    dg.generateAndSendData();
                } catch (Exception e) {
                    System.err.println("Error while starting DataGenerator: " + e);
                    System.exit(1);
                }
            }

            if (line.hasOption("consumer")) {
                try {
                    SimpleConsumer cs = new SimpleConsumer(propFile);
                    cs.consume();
                } catch (Exception e) {
                    System.err.println("Error while starting Consumer: " + e);
                    System.exit(1);
                }
            }

        }
        catch( ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }
    }
}
