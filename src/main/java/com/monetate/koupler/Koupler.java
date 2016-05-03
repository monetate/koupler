package com.monetate.koupler;

import java.io.BufferedReader;
import java.net.SocketException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Class, and super class to all the koupler implementations.
 *
 * @author brianoneill
 */
public abstract class Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Koupler.class);
    public static final int DEFAULT_BACKOFF = 10; // (in ms)
    public static final int MAX_BACKOFF = 10000; // ten seconds
    private static final Random RANDOM = new Random();

    public KinesisEventProducer producer;
    private ExecutorService threadPool;

    public Koupler(KinesisEventProducer producer, int threadPoolSize) {
        this.producer = producer;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
    }

    public ExecutorService getThreadPool() {
        return this.threadPool;
    }

    class KouplerThread implements Callable<Integer> {
        private BufferedReader bufferedReader;
        private ExceptionHandler exceptionHandler;
        private boolean running = true;
        private int backOff = DEFAULT_BACKOFF;

        public KouplerThread(BufferedReader bufferedReader, ExceptionHandler exceptionHandler) {
            this.bufferedReader = bufferedReader;
            this.exceptionHandler = exceptionHandler;
        }

        public KouplerThread(BufferedReader bufferedReader) {
            this(bufferedReader, null);
        }

        @Override
        public Integer call() {
            int numOfEvents = 0;
            Exception error = null;
            while (running) {
                try {
                    String event = bufferedReader.readLine();
                    if (event != null) {
                        LOGGER.debug("Queueing event [{}]", event);
                        producer.queueEvent(event);
                        numOfEvents++;
                    } else {
                        LOGGER.debug("Received null event, dropping socket.");
                        running = false;
                    }
                    backOff = DEFAULT_BACKOFF;
                } catch (EventQueueFullException eqfe) {
                    String msg = String.format(
                            "Internal event queue is full, ingest is outpacing egress. (queue.size=[%d])",
                            eqfe.getSize());
                    LOGGER.error(msg);
                    LOGGER.error("WARNING: Dropping events on floor.");
                    // In this case, it is a valid event, we just don't have
                    // room for it
                    // So we'll continue running until we have room, but will
                    // delay to see if the queue clears.
                    try {
                        LOGGER.warn("Sleeping {}ms waiting for queue to clear.", backOff);
                        Thread.sleep(backOff);
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Insomnia -- can't sleep!", ie);
                    }
                    backOff = Math.min(MAX_BACKOFF, backOff * 2) + RANDOM.nextInt(100);
                } catch (Exception e) {
                    LOGGER.error("Erroring reading/queuing event [{}]", e);
                    running = false;
                    error = e;
                }
            }
            if (exceptionHandler != null)
                exceptionHandler.handleException(error);
            return numOfEvents;
        }
    }

    public static void main(String[] args) throws ParseException, SocketException {
        boolean misconfigured = false;
        Options options = new Options();

        String propertiesFile = "./conf/kpl.properties";
        options.addOption("propertiesFile", true, "kpl properties file (default: " + propertiesFile + ")");

        int port = 4242;
        options.addOption("port", true, "listening port (default: " + port + ")");
        options.addOption("partitionKeyField", true,
                "field containing partition key (default: " + 0 + ")");

        options.addOption("format", true, "format of data (default: 'split')");
        options.addOption("delimiter", true, "delimiter between fields (default: ',')");
        options.addOption("udp", false, "udp mode");
        options.addOption("http", false, "http mode");
        options.addOption("tcp", false, "tcp mode");
        options.addOption("pipe", false, "pipe mode");
        options.addOption("consumer", false, "consumer mode");
        options.addOption("streamName", true, "kinesis stream name");
        options.addOption("appName", true, "app/consumer name");
        options.addOption("position", true, "initial position in stream (default: LATEST)");
        options.addOption("metrics", false, "publish metrics to cloudwatch");
        options.addOption("queueSize", true, "event buffer/queue size (default: 50000)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("propertiesFile")) {
            propertiesFile = cmd.getOptionValue("propertiesFile");
        }
        if (cmd.hasOption("port")) {
            port = Integer.parseInt(cmd.getOptionValue("port"));
        }

        String initialPosition = "LATEST";
        if (cmd.hasOption("position")) {
            initialPosition = cmd.getOptionValue("position");
        }

        // Check to see they specified one of (udp, tcp http, or pipe)
        if (!cmd.hasOption("udp") && !cmd.hasOption("tcp") && !cmd.hasOption("http") && !cmd.hasOption("pipe")
                && !cmd.hasOption("consumer")) {
            System.err.println("Must specify either: udp, http, tcp, pipe, or consumer");
            misconfigured = true;
        }

        String streamName = "";
        if (!cmd.hasOption("streamName")) {
            System.err.println("Must specify stream name.");
            misconfigured = true;
        } else {
            streamName = cmd.getOptionValue("streamName");
        }

        int queueSize = 50000;
        if (cmd.hasOption("queueSize")) {
            queueSize = Integer.parseInt(cmd.getOptionValue("queueSize"));
        }

        if (misconfigured) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp("java -jar koupler*.jar", options);
            System.exit(-1);
        }

        String appName = "koupler";
        if (cmd.hasOption("appName")) {
            appName = cmd.getOptionValue("appName");
        }

        String format = "split";
        if (cmd.hasOption("format")) {
            appName = cmd.getOptionValue("format");
        }

        KinesisEventProducer producer = new KinesisEventProducer(format, cmd, propertiesFile, streamName, queueSize, appName);
        if (cmd.hasOption("metrics")) {
            producer.startMetrics();
        }

        Koupler koupler = null;
        boolean server = true;
        if (cmd.hasOption("tcp")) {
            koupler = new TcpKoupler(producer, port);
        } else if (cmd.hasOption("udp")) {
            koupler = new UdpKoupler(producer, port);
        } else if (cmd.hasOption("http")) {
            koupler = new HttpKoupler(producer, port);
        } else if (cmd.hasOption("pipe")) {
            koupler = new PipeKoupler(producer);
        } else if (cmd.hasOption("consumer")) {

            KinesisEventConsumer consumer = new KinesisEventConsumer(propertiesFile, streamName, appName,
                    initialPosition);
            consumer.start();
        }

        if (server) {
            Thread producerThread = new Thread(producer);
            producerThread.start();

            Thread kouplerThread = new Thread(koupler);
            kouplerThread.start();
        }
    }

}
