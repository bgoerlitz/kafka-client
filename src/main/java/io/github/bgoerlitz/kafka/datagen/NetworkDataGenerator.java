package io.github.bgoerlitz.kafka.datagen;

import io.github.bgoerlitz.kafka.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class NetworkDataGenerator extends AbstractDataGenerator {
    Logger logger = LoggerFactory.getLogger(NetworkDataGenerator.class);

    private static final String PORT_CONF = "listener-port";

    private static final String DEFAULT_PORT = "9999";

    private int portNumber;


    public NetworkDataGenerator(String propFile)
    {
        super(propFile);
    }


    void configure(String propFile)
    {
        super.configure(propFile);

        portNumber = Integer.parseInt((String) properties.getOrDefault(PORT_CONF, DEFAULT_PORT));

        createProducer(propFile);

    }

    @Override
    public void generateAndSendData() {
        try (
                ServerSocket serverSocket = new ServerSocket(portNumber);
        ) {
            String inputLine;
            boolean running = true;
            while (running) {
                try (
                        Socket clientSocket = serverSocket.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        ) {
                    if ((inputLine = in.readLine()) != null) {
                        producer.produceData(inputLine);
                    }

                } catch (Exception e) {
                    logger.error("Exception in client connection on port " + portNumber, e);
                }
            }
        } catch (Exception e) {
            logger.error("Exception in NetworkDataGenerator on port " + portNumber, e);
        }
    }

    public static void main(String[] args) {
        AbstractDataGenerator dg = new NetworkDataGenerator(ConfigUtil.DEFAULT_PROPERTY_FILE);
        dg.generateAndSendData();
    }
}
