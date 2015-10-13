package com.monetate.koupler;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeKoupler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventProducer.class);

    public static void main(String args[]) throws Exception {
        KinesisEventProducer factProducer = null;
        if (args.length == 2) {
            LOGGER.info("Reading from STDIN.");
            String streamName = args[0];
            String propertiesFile = args[1];
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
            factProducer = new KinesisEventProducer(propertiesFile, streamName);
            factProducer.setBufferedReader(bufferedReader);
        } else {
            System.out.println("Usage: java -jar stream-tributary.jar $STREAM $PROPERTIES_FILE");
            System.out.println("e.g. java -jar stream-tributary.jar $stream $properties_file");
            System.exit(1);
        }

        Thread runThread = new Thread(factProducer);
        runThread.start();
        runThread.join();
    }

}
